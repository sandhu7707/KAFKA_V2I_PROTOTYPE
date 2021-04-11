package com.example.Car;

import com.example.Car.Model.MessageProps;
import com.example.Car.Model.MessageQueue;
import com.example.Car.RabbitMQ.RabbitMQSimpleReceive;
import com.example.Car.RabbitMQ.RabbitMQSimpleSend;
import com.rabbitmq.client.*;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.awt.image.AreaAveragingScaleFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class Car {

    CarProps carProps;
    SensorData sensorData;

    public static String EVALUATION_MODULE_ADDRESS = "localhost";

    Car(CarProps carProps, JSONArray topics) throws JSONException, IOException, TimeoutException, InterruptedException {
        this.sensorData = new SensorData();
        this.carProps = carProps;
        Thread t = new Thread(() -> {
            try {
                startMoving();
            } catch (InterruptedException | JSONException | IOException | TimeoutException e) {
                e.printStackTrace();
            }
        });
        t.start();

        if ((carProps.mode).equals(MessageProps.EVALUATION)) {

            //this function will receive simulated messages and send it to RSU
            startReceiverOnSimulatedMessages(topics);

        } else {

            System.out.println("Only Evaluation Mode is supported for this version! Evaluation Mode should only be triggered from EVALUATION module!");
            //TODO: ususal AVFORDDATASET stuff
        }
    }

    /*
    Simulated movement
 */
    private void startMoving() throws InterruptedException, JSONException, IOException, TimeoutException {
        Thread.sleep(carProps.tickTime * 1000);

        //this function is doing all the work here..
        carProps.updateCarPositionAndSpeed();

//        System.out.println("position of " + carProps.carId + ": " + carProps.position);
//        System.out.println("speed of " + carProps.carId + ": " + carProps.speed);
//        System.out.println("nearestRSUAddress of " + carProps.carId + ": " + carProps.nearestRSUAddress);

        if (carProps.position < 2000) {
            startMoving();     //this will keep us moving
        } else {
            System.out.println(carProps.carId + " has finished it's journey !");
        }
    }

    private void startReceiverOnSimulatedMessages(JSONArray topics) throws JSONException, IOException, TimeoutException, InterruptedException {
        //connectingTORSU
        Thread t = new Thread(() -> {
            try {
                connectOnRSUAndStartSending();
            } catch (InterruptedException | JSONException | IOException | TimeoutException e) {
                e.printStackTrace();
            }
        });
        t.start();

        //starting receivers
        t = new Thread(() -> {
            try {
                startReceiversOn(topics);
            } catch (IOException | TimeoutException | JSONException e) {
                e.printStackTrace();
            }
        });
        t.start();

        //sending INIT to trigger simulated messages
        String carToControllerQName = carProps.getCarToControllerMessageQ();
        RabbitMQSimpleSend sender = new RabbitMQSimpleSend(carToControllerQName, EVALUATION_MODULE_ADDRESS);

        JSONObject message_json = new JSONObject();
        message_json.put(MessageProps.TYPE, MessageProps.INIT);

        sender.sendMessage(message_json.toString());

    }

    // .. stop receivers when more than 5 messages are dropped, connect to new RSU and start again..
    //this function should be able to restart producers on new RSU..

    private void startReceiversOn(JSONArray topics) throws IOException, TimeoutException, JSONException {

        for (int i = 0; i < topics.length(); i++) {
            int finalI = i;
            Thread t = new Thread(() -> {

                String topic = null;
                try {
                    topic = (String) topics.get(finalI);
                } catch (JSONException e) {
                    e.printStackTrace();
                }

                RabbitMQSimpleReceive receiver = new RabbitMQSimpleReceive(carProps.getTopicQForCarFromTopicName(topic), EVALUATION_MODULE_ADDRESS);
                try {

                    receiver.receive((s, delivery) -> {
                        String message = new String(delivery.getBody(), "UTF-8");
                        try {
                            handleMessage(message);

                        } catch (JSONException | InterruptedException e) {
                            e.printStackTrace();
                        }
                    });
                } catch (IOException | TimeoutException e) {
                    e.printStackTrace();
                }

            });

            t.start();
        }
    }

    HashMap<String, ArrayList<String>> newDataOnTopic = new HashMap<>();

    private void handleMessage(String message) throws JSONException, InterruptedException {
        JSONObject message_json = new JSONObject(message);
        String topic = (String) message_json.get("TOPIC");

        //TODO: check if the parallel access works.. something's wrong.. this doesn't work

        if (sensorData.sensorDataByTopics.containsKey(topic)) {
            ArrayList<String> newData = newDataOnTopic.get(topic);

            newData.add(message);

            if (sensorData.checkAccessToSensorDataOnTopic(topic)) {
                sensorData.acquireAccessOnTopic(topic);
                sensorData.updateSensorDataOnTopic(topic, newData);
                sensorData.looseAccessOnTopic(topic);
                newData = new ArrayList<>();
            }

            newDataOnTopic.replace(topic, newData);
        } else {
            sensorData.sensorDataByTopics.put(topic, new ArrayList<>(Collections.singleton(message)));
            newDataOnTopic.put(topic, new ArrayList<>());
        }
    }

    private volatile int initState = 3;

    private void connectOnRSUAndStartSending() throws InterruptedException, JSONException, IOException, TimeoutException {
        while (carProps.nearestRSUAddress.equals("n/a")) {
            //do nothing
            if (!carProps.nearestRSUAddress.equals("n/a")) {
                carProps.RSUGotSwitched = true;
                carProps.RSUNeedsSwitch = false;
                break;
            }
        }

        initState = 1;

        Thread t = new Thread(() -> {
            try {
                startReceiverForInitByRSU();
            } catch (IOException | TimeoutException | InterruptedException e) {
                e.printStackTrace();
            }
        });

        t.start();

        while (initState != 3) {
            System.out.println(initState);
            if (initState == 3) {
                break;
            }
            initWithRSU();
            Thread.sleep(1000);
        }

        System.out.println("will start sending on topics now........................");
        startSendingSensorData();

    }


    private void startReceiverForInitByRSU() throws IOException, TimeoutException, InterruptedException {
        RabbitMQSimpleReceive simpleReceiver = new RabbitMQSimpleReceive(carProps.getRSUToCarInitQ(), carProps.nearestRSUAddress);
        simpleReceiver.receive(new DeliverCallback() {
            @Override
            public void handle(String s, Delivery delivery) throws IOException {
                String message = new String(delivery.getBody(), "UTF-8");
                try {
                    JSONObject message_json = new JSONObject(message);
                    if (((String) message_json.get(MessageProps.TYPE)).equals(MessageProps.INIT)) {
                        initState = 3;
                    }

                } catch (JSONException e) {
                    e.printStackTrace();
                }

            }
        });
    }

    private void initWithRSU() throws InterruptedException, JSONException {
        RabbitMQSimpleSend sender = new RabbitMQSimpleSend(MessageQueue.carToRSUInitQ, carProps.nearestRSUAddress);
        String message = getRSUInitMessage();
        sender.sendMessage(message);
        System.out.println("sent init message to RSU -> " + message);
        initState = 2;
    }

    // TODO: this function should do the init with RSU, exchange consumer topics, .. so it can start receiving.., inform RSU of the simulatedMessage topic
    // connectToNearestRSU should start producers to forward received messages.. (10 (can make this decidable by Evaluation later maybe..) consecutive packet drops -> check if RSU needs switched -> if yes -> restart producers)
    // TODO: flip carProps.RSUGotSwitched when reconnected to a new RSU..
    // send forced update to Evaluation module when the switch is flipped.. (if necessary)

    // connectToNearestRSU(); -> INIT and stuff


    private String getRSUInitMessage() throws JSONException {

        JSONObject message_json = new JSONObject();
        message_json.put(MessageProps.TYPE, MessageProps.INIT);
        message_json.put(MessageProps.TOPICS, carProps.sensorTopics);
        message_json.put(MessageProps.CAR_ID, carProps.carId);

        return message_json.toString();
    }

    private void startSendingSensorData() {
        JSONArray topics = carProps.sensorTopics;
        Thread t;
        for (int i = 0; i < topics.length(); i++) {
            int finalI = i;
            t = new Thread(() -> {
                if (carProps.messagePriority == 1) {                                                                          //TODO: add other priorities
                    try {
                        sendDataToRSUOnTopicWithAsyncAcks((String) topics.get(finalI));
                    } catch (JSONException e) {
                        e.printStackTrace();
                    }
                }
            });
            t.start();
        }
    }

    RetransmissionsByTopics retransmissionsByTopic = new RetransmissionsByTopics();
    private void sendDataToRSUOnTopicWithAsyncAcks(String topic) {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(carProps.nearestRSUAddress);
        AtomicLong consecutiveConfirmsCounter = new AtomicLong();
        String qName = carProps.getCarToRSUQForTopic(topic);
        HashMap<Long, String> outstandingConfirms = new HashMap<>();
        AtomicLong maxClearedSequence = new AtomicLong();
        HashMap<String, String> transmittedMessages = new HashMap<>();

        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {

            channel.confirmSelect();


            channel.addConfirmListener((sequenceNumber, multiple) -> {
                System.out.println(carProps.consecutiveSuccessfulMessagesBetweenDrops);
                System.out.println(consecutiveConfirmsCounter.get());
                ArrayList<String> retransmissions = new ArrayList<>();
                if(!multiple) {
                    consecutiveConfirmsCounter.getAndIncrement();
                    if (carProps.consecutiveSuccessfulMessagesBetweenDrops <= consecutiveConfirmsCounter.get()) {
                        try {
                            JSONObject message_json = new JSONObject(outstandingConfirms.get(sequenceNumber));
                            System.out.println("added to retransmission -> " + message_json.toString());
                            message_json.put(MessageProps.SIMULATED_DROP_RETRANSMISSION_ATTEMPT, ((int)message_json.get(MessageProps.SIMULATED_DROP_RETRANSMISSION_ATTEMPT) + 1));
                            retransmissions.add(message_json.toString());
                            outstandingConfirms.remove(sequenceNumber);
                            consecutiveConfirmsCounter.getAndSet(0);
                        } catch (JSONException e) {
                            e.printStackTrace();
                        }
                    }
                }
                else{
                    long delta = sequenceNumber - maxClearedSequence.get();
                    consecutiveConfirmsCounter.getAndAdd(delta);
                    if(carProps.consecutiveSuccessfulMessagesBetweenDrops <= consecutiveConfirmsCounter.get()) {
                        for(int i = 0; consecutiveConfirmsCounter.get() - i >= carProps.consecutiveSuccessfulMessagesBetweenDrops; i++){
                            try {
                                JSONObject message_json = new JSONObject(outstandingConfirms.get(sequenceNumber-i));
                                System.out.println("added to retransmission -> " + message_json.toString());
                                message_json.put(MessageProps.SIMULATED_DROP_RETRANSMISSION_ATTEMPT, ((int)message_json.get(MessageProps.SIMULATED_DROP_RETRANSMISSION_ATTEMPT) + 1));
                                retransmissions.add(message_json.toString());
                                outstandingConfirms.remove(sequenceNumber - i);
                                consecutiveConfirmsCounter.getAndSet(0);
                        } catch (JSONException e) {
                                e.printStackTrace();
                            }
                        }
                    }
                }
                maxClearedSequence.set(sequenceNumber);
                try {
                    retransmissionsByTopic.updateRetransmissionDataOnTopic(topic, retransmissions);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

//                System.out.println(sequenceNumber);
//                System.out.println(multiple);
//                System.out.println("... acked !");
                }, (sequenceNumber, multiple) -> {

                ArrayList<String> retransmissions = new ArrayList<>();
                if(!multiple) {
                    consecutiveConfirmsCounter.getAndIncrement();
                        try {
                            JSONObject message_json = new JSONObject(outstandingConfirms.get(sequenceNumber));
                            message_json.put(MessageProps.REAL_DROP_RETRANSMISSION_ATTEMPT, ((int)message_json.get(MessageProps.REAL_DROP_RETRANSMISSION_ATTEMPT) + 1));
                            System.out.println("added to retransmission -> " + message_json.toString());
                            retransmissions.add(message_json.toString());
                            outstandingConfirms.remove(sequenceNumber);
                            consecutiveConfirmsCounter.getAndSet(0);
                        } catch (JSONException e) {
                            e.printStackTrace();
                        }
                    }
                else{
                    long delta = sequenceNumber - maxClearedSequence.get();
                    consecutiveConfirmsCounter.getAndAdd(delta);
                        for(int i = 0; consecutiveConfirmsCounter.get() - i >= carProps.consecutiveSuccessfulMessagesBetweenDrops; i++){
                            try {
                                JSONObject message_json = new JSONObject(outstandingConfirms.get(sequenceNumber-i));
                                message_json.put(MessageProps.REAL_DROP_RETRANSMISSION_ATTEMPT, ((int)message_json.get(MessageProps.REAL_DROP_RETRANSMISSION_ATTEMPT) + 1));
                                System.out.println("added to retransmission -> " + message_json.toString());
                                retransmissions.add(message_json.toString());
                                outstandingConfirms.remove(sequenceNumber - i);
                                consecutiveConfirmsCounter.getAndSet(0);
                            } catch (JSONException e) {
                                e.printStackTrace();
                            }
                        }
                }

                maxClearedSequence.set(sequenceNumber);
                try {
                    retransmissionsByTopic.updateRetransmissionDataOnTopic(topic, retransmissions);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

//                    System.out.println(sequenceNumber);
//                    System.out.println(multiple);
//                    System.out.println("... nacked !");
            });

            channel.queueDeclare(qName, true, false, false, null);

            //update evaluation module on transmitted messageIds when update is done .. do similar thing in RSU and services
            //transmit to RSU with 1. sentFromCarAt timestamp 2. simulatedDropRetransmissionAttemptNumber 3. realDropRetransmissionAttemptNumber
            //add logic in rsu to handle retransmissions
            //In RSU, add 1. receivedAtRSU timestamp 2. sentFromRSUAt timestamp
            //In Services, add receivedAtServices timestamp

            while(true) {

                ArrayList<String> retransmissions = retransmissionsByTopic.getRetransmissionDataOnTopicAndEmptyBuffer(topic);

                carProps.transmittedDataRecord.updateTransmittedDataOnTopic(topic, transmittedMessages);
                transmittedMessages = new HashMap<>();

                ArrayList<String> dataOnTopic = new ArrayList<>(retransmissions);
                retransmissions = new ArrayList<>();

                while (!sensorData.checkAccessToSensorDataOnTopic(topic)) {
                    Thread.sleep(1);
                }
                sensorData.acquireAccessOnTopic(topic);
                dataOnTopic.addAll(sensorData.sensorDataByTopics.get(topic));
//                        System.out.println("sensorDataByTopics -> " + sensorData.sensorDataByTopics);
//                        System.out.println("dataOnTopic -> " + dataOnTopic);
                sensorData.sensorDataByTopics.replace(topic, new ArrayList<>());
                sensorData.looseAccessOnTopic(topic);


                if(carProps.RSUNeedsSwitch && !carProps.RSUGotSwitched){
                    //finalize/cache outstanding retransmissions and outstanding confirms..

                    carProps.transmittedDataRecord.updateTransmittedDataOnTopic(topic, transmittedMessages);

                    carProps.RSUNeedsSwitch = false;
                    carProps.RSUGotSwitched = true;

                    while (!sensorData.checkAccessToSensorDataOnTopic(topic)) {
                        Thread.sleep(1);
                    }

                    retransmissions.addAll(outstandingConfirms.values());

                    sensorData.acquireAccessOnTopic(topic);
                    retransmissions.addAll(sensorData.sensorDataByTopics.get(topic));
                    sensorData.sensorDataByTopics.replace(topic, retransmissions);
                    sensorData.looseAccessOnTopic(topic);

                    Thread t = new Thread(()-> {
                        try {
                            connectOnRSUAndStartSending();
                        } catch (InterruptedException | JSONException | IOException | TimeoutException e) {
                            e.printStackTrace();
                        }
                    });

                    t.start();

                    break;
                }



//                  System.out.println("sending data: " + dataOnTopic);

                for (String message: dataOnTopic) {


                    JSONObject message_json = new JSONObject(message);
                    String messageWithHeaders = addHeadersToMessageData(message_json);

                    transmittedMessages.put((String) message_json.get(MessageProps.MESSAGE_ID), messageWithHeaders);

                    outstandingConfirms.put(channel.getNextPublishSeqNo(), messageWithHeaders);

                    channel.basicPublish("", qName,
                            MessageProperties.PERSISTENT_TEXT_PLAIN,
                            messageWithHeaders.getBytes("UTF-8"));

//                    System.out.println(" [x] Sent '" + messageWithHeaders + "'");

                    Thread.sleep(1);                                                                                     //TODO: try eliminating this

                }

            }
            System.out.println("ended the stream to RSU, will connect to the next one..");                                      //can't see this since all the addresses are same and switch never happens !

        } catch (IOException | TimeoutException | InterruptedException | JSONException e) {
            e.printStackTrace();
        }

    }

    private String addHeadersToMessageData(JSONObject message_json) throws JSONException {
        if(!message_json.has(MessageProps.SENT_FROM_CAR_AT)) {
            message_json.put(MessageProps.SIMULATED_DROP_RETRANSMISSION_ATTEMPT, 0);
            message_json.put(MessageProps.REAL_DROP_RETRANSMISSION_ATTEMPT, 0);
        }
        message_json.put(MessageProps.SENT_FROM_CAR_AT, System.currentTimeMillis());

        return message_json.toString();
    }

}
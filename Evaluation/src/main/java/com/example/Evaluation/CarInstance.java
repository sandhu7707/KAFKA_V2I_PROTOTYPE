package com.example.Evaluation;

import com.example.Evaluation.Model.MessageProps;
import com.example.Evaluation.Model.MessageQueue;
import com.example.Evaluation.RabbitMQ.RabbitMQSimpleReceive;
import com.example.Evaluation.RabbitMQ.RabbitMQSimpleSend;
import com.rabbitmq.client.*;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

public class CarInstance {

    private double dropConstant;
    private final String carId = UUID.randomUUID().toString();
    private JSONArray topics;
    private RabbitMQSimpleSend sender;
    private SimulateMessages messageSimulator;
    private int delay = 50;
    private EvaluationIndex evaluationIndex;

    JSONArray getTopicNames(int numberOfTopics){
        JSONArray topics = new JSONArray();
        for(int i=0; i<numberOfTopics; i++){
            topics.put(MessageQueue.TOPIC_NAME_PREFIX + i);
        }
        this.topics = topics;
        return topics;
    }

    void StartCarInstance(double dropConstant, String mode, int messagePriority, int numberOfTopics, int messageSize) throws JSONException, IOException, TimeoutException {
        this.dropConstant = dropConstant;
        this.messageSimulator = new SimulateMessages(messageSize);
        JSONArray topics = getTopicNames(numberOfTopics);
        this.evaluationIndex = new EvaluationIndex(carId, topics);

        RabbitMQSimpleSend initSender = new RabbitMQSimpleSend(MessageQueue.carInitQ);

        JSONObject message_json = new JSONObject();
        message_json.put(MessageProps.MESSAGE, "");
        message_json.put(MessageProps.TYPE, MessageProps.INIT);
        message_json.put(MessageProps.CAR_ID, carId);
        message_json.put(MessageProps.MODE, mode);
        message_json.put(MessageProps.PRIORITY, messagePriority);
        message_json.put(MessageProps.TOPICS, topics);
        initSender.sendMessage(message_json.toString());

        startReceiverForCarInstance();
    }

    // simulated data will be sent starting with this function, as soon as INIT is received from started car Instance.
    /**
     * In case we wanna do multiple topics, this is where we start looking around...
     * **/
    void startReceiverForCarInstance() throws IOException, TimeoutException {
        RabbitMQSimpleReceive receiver = new RabbitMQSimpleReceive(getCarToControllerMessageQ());
        receiver.receive(new DeliverCallback() {
            @Override
            public void handle(String s, Delivery delivery) throws IOException {
                String message = new String(delivery.getBody(),"UTF-8");

                System.out.print("received message from " + carId +" -> " );
                System.out.println(message);

                try {
                    JSONObject message_json = new JSONObject(message);
                    String message_type = (String) message_json.get(MessageProps.TYPE);
                    if(message_type.equals(MessageProps.INIT)){
                        sendSimulatedData();
                    }

                    else if(message_type.equals(MessageProps.UPDATE)){
                        actOnMessageUpdate((JSONObject) message_json.get(MessageProps.DATA));
                    }

                } catch (JSONException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    void sendSimulatedData() throws JSONException {
        for(int i=0; i<topics.length(); i++){
            int finalI = i;

            Thread t = new Thread(() -> {
                String topic = null;
                try {
                    topic = (String) topics.get(finalI);
                    sendSimulatedDataOnTopic(topic);
                } catch (JSONException | InterruptedException e) {
                    e.printStackTrace();
                }

            });

            t.start();
        }
    }

    void sendSimulatedDataOnTopic(String topic) throws JSONException, InterruptedException {
        String simulatedMessageData = messageSimulator.simulatedMessageData;
        JSONObject message_json = new JSONObject();
        message_json.put(MessageProps.DATA, simulatedMessageData);
        message_json.put(MessageProps.TOPIC, topic);
        System.out.println("data will be sent for " + topic);

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(EvaluationApplication.EVALUATION_APPLICATION_ADDRESS);
        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {

            String qName = getTopicQForCarFromTopicName(topic);
            channel.queueDeclare(qName, true, false, false, null);

            while(!EvaluationApplication.stopSimulation) {
                recursiveSender(message_json, channel, qName);
            }

        } catch (TimeoutException | IOException e) {
            e.printStackTrace();
        }
    }

    private void recursiveSender(JSONObject message_json, Channel channel, String qName) throws JSONException, InterruptedException, IOException {
        message_json.put(MessageProps.MESSAGE_ID, UUID.randomUUID().toString() + "_" + carId);

//        System.out.print("sending simulated message -> ");
//        System.out.println(message_json.toString());

        channel.basicPublish("", qName,
                MessageProperties.PERSISTENT_TEXT_PLAIN,
                message_json.toString().getBytes("UTF-8"));

        Thread.sleep(delay);

    }

    void actOnMessageUpdate(JSONObject message_data_from_car) throws JSONException {                                        //TODO: use these messages to update car position in simulation

        sender = new RabbitMQSimpleSend(getControllerToCarMessageQ());

        String RSUAddress = (String) message_data_from_car.get(MessageProps.NEAREST_RSU_ADDRESS);
        Integer position = (Integer) message_data_from_car.get(MessageProps.POSITION);
        String newRSUAddress = automatedRSUAllocator(position, RSUAddress, carId);
        Boolean RSUWasSwitched = (Boolean) message_data_from_car.get(MessageProps.RSU_WAS_SWITCHED);

        JSONObject message_json = new JSONObject();

        message_json.put(MessageProps.TYPE, MessageProps.UPDATE);
        //gets up to date RSU address and drop rate and sends it..
        message_json.put(MessageProps.NEAREST_RSU_ADDRESS, newRSUAddress);

//        boolean isRSUChanged = RSUAddress.equals(newRSUAddress);

        double dropRate = getDropRate(position);

        if(RSUWasSwitched){
            isRSUChanged = false;
        }
        else  if(isRSUChanged){
            dropRate = 1.0;
        }

        message_json.put(MessageProps.RSU_NEEDS_SWITCH, isRSUChanged);
        message_json.put(MessageProps.CONSECUTIVE_SUCCESSFUL_MESSAGES, getConsecutiveSuccessfulMessagesBetweenDrops(dropRate));

        sender.sendMessage(message_json.toString());
        System.out.println("sent a message -> " + message_json.toString());
    }

    private boolean isRSUChanged = false;

    double getDropRate(Integer position){
        int rangeOfRSU = 300;
        int distanceFromNearestRSU = Math.abs((position % (2*rangeOfRSU)) - rangeOfRSU);     //fix this
        return (dropConstant + ((1 - dropConstant)*(distanceFromNearestRSU)/(rangeOfRSU+50)));                   //TODO: change this formula -- this is wrong
    }

    int getConsecutiveSuccessfulMessagesBetweenDrops(double dropRate){
        return (int)((maxConsecutiveNacks/dropRate) - maxConsecutiveNacks);
    }
    double maxConsecutiveNacks = 15.0;

    /*
    * This logic makes an assumption that our RSUs are located at 250, 750, 1250 and 1750
    *
    * */
    String automatedRSUAllocator(int position, String nearestRSUAddress, String carId){

        if(0 <= position && position < 500){
            if(nearestRSUAddress == null || !nearestRSUAddress.equals(firstRSUAddress)){
                System.out.println(carId + " can now only produce on RSU @ " + firstRSUAddress );
                isRSUChanged = true;
            }
            nearestRSUAddress = firstRSUAddress;
        }

        else if(500 <= position && position < 1000){
            if(!nearestRSUAddress.equals(secondRSUAddress)){
                System.out.println(carId + " can now only produce on RSU @ " + secondRSUAddress );
                isRSUChanged = true;
            }
            nearestRSUAddress = secondRSUAddress;
        }

        else if(1000 <= position && position < 1500){
            if(!nearestRSUAddress.equals(thirdRSUAddress)){
                System.out.println(carId + " can now only produce on RSU @ " + thirdRSUAddress );
                isRSUChanged = true;
            }
            nearestRSUAddress = thirdRSUAddress;
        }

        else if(1500 <= position && position < 2000){
            if(!nearestRSUAddress.equals(fourthRSUAddress)){
                System.out.println(carId + " can now only produce on RSU @ " + fourthRSUAddress );
                isRSUChanged = true;
            }
            nearestRSUAddress = fourthRSUAddress;
        }

        return nearestRSUAddress;
    }

    private static final String firstRSUAddress = "localhost";                  //change these addresses to device addresses
    private static final String secondRSUAddress = "localhost";
    private static final String thirdRSUAddress = "localhost";
    private static final String fourthRSUAddress = "localhost";


    private String getCarToControllerMessageQ(){
        return "C_TO_CO_" + carId;
    }

    private String getControllerToCarMessageQ(){
        return "CO_TO_C_" + carId;
    }

    private String getTopicQForCarFromTopicName(String topic){
        return topic + carId;
    }

}



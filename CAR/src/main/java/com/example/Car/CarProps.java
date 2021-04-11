package com.example.Car;

import com.example.Car.Model.MessageProps;
import com.example.Car.Model.MessageQueue;
import com.example.Car.RabbitMQ.RabbitMQSimpleReceive;
import com.example.Car.RabbitMQ.RabbitMQSimpleSend;
import com.rabbitmq.client.DeliverCallback;
import com.rabbitmq.client.Delivery;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.TimeoutException;

public class CarProps {

    int position;
    int speed;
    volatile String nearestRSUAddress;
    public final int tickTime = 2;
    public String carId;
    public volatile int consecutiveSuccessfulMessagesBetweenDrops;
    public String mode;
    public volatile boolean RSUNeedsSwitch = false;
    public volatile boolean RSUGotSwitched = false;                                                      //can be set true only by car processes sending data..
    public int messagePriority;
    public RabbitMQSimpleSend sender;
    JSONArray sensorTopics;
    TransmittedDataRecord transmittedDataRecord;

    CarProps(String mode, String carId, int messagePriority, JSONArray topics){
        this.transmittedDataRecord = new TransmittedDataRecord();
        position = 0;
        speed = (int) ((Math.random() * (30)) + 5);  // speed between 5 to 35 m/s
        this.mode = mode;
        this.carId = carId;
        this.messagePriority = messagePriority;
        this.sensorTopics = topics;
        this.nearestRSUAddress = "n/a";
        sender = new RabbitMQSimpleSend(getCarToControllerMessageQ(), Car.EVALUATION_MODULE_ADDRESS);

        Thread t = new Thread (()->{
            try {
                startReceiverOnEvaluationModuleForUpdates();
            } catch (IOException | TimeoutException e) {
                e.printStackTrace();
            }
        });
        t.start();

    }

    void updateCarPositionAndSpeed() throws JSONException, IOException, TimeoutException {
//        int lastPosition = position;
        position = position + (speed * tickTime);
        speed =  (int) ((Math.random() * (30)) + 15);

        /************************************************
        this.nearestRSUAddress = automatedRSUAllocator(position, nearestRSUAddress, carId);

        if((int)position/500 != (int)lastPosition/500){                                                         //won't need this part when addresses of RSUs are different
            this.RSUGotSwitched = true;
        }
****************************/
        updateOnEvaluationModule();
    }

    private void startReceiverOnEvaluationModuleForUpdates() throws IOException, TimeoutException {
        RabbitMQSimpleReceive receiver = new RabbitMQSimpleReceive(getControllerToCarMessageQ(), Car.EVALUATION_MODULE_ADDRESS);
        receiver.receive(new DeliverCallback() {
            @Override
            public void handle(String s, Delivery delivery) throws IOException {
                String message = new String(delivery.getBody(), "UTF-8");
                System.out.print("New UPDATE message -> ");
                System.out.println(message);
                try {
                    JSONObject message_json = new JSONObject(message);
                    nearestRSUAddress = (String) message_json.get(MessageProps.NEAREST_RSU_ADDRESS);
//                    System.out.println(message_json.get(MessageProps.DROP_RATE));

                    consecutiveSuccessfulMessagesBetweenDrops = (int) message_json.get(MessageProps.CONSECUTIVE_SUCCESSFUL_MESSAGES);
//                    System.out.println(dropRate);
                    System.out.println(consecutiveSuccessfulMessagesBetweenDrops);
                    RSUNeedsSwitch = (boolean) message_json.get(MessageProps.RSU_NEEDS_SWITCH);

                } catch (JSONException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    void updateOnEvaluationModule() throws JSONException {
        JSONObject message_json = new JSONObject();
        message_json.put(MessageProps.TYPE, MessageProps.UPDATE);

        JSONObject message_data = new JSONObject();
        message_data.put(MessageProps.POSITION, position);
        message_data.put(MessageProps.SPEED, speed);
        message_data.put(MessageProps.NEAREST_RSU_ADDRESS, nearestRSUAddress);
        message_data.put(MessageProps.RSU_WAS_SWITCHED, RSUGotSwitched);
        message_json.put(MessageProps.DATA, message_data);

        //do a function in transmittedData called getAndEmptyBuffer(), send it to evaluation module and start keeping a record there
        //fix code in RSU, do the same update thing in RSU too..
        //services -> reverse flow !

        sender.sendMessage(message_json.toString());

        if(RSUGotSwitched){
            RSUGotSwitched = false;
        }

        Thread t = new Thread(() -> {
            try {
                updateEvaluationModuleOnTransmittedMessages();
            } catch (JSONException e) {
                e.printStackTrace();
            }
        });
        t.start();
    }

    private void updateEvaluationModuleOnTransmittedMessages() throws JSONException {
        for (int i = 0; i < sensorTopics.length(); i++) {
            int finalI = i;
            Thread t = new Thread (() -> {
                String topic = null;
                try {
                    topic = (String) sensorTopics.get(finalI);
                } catch (JSONException e) {
                    e.printStackTrace();
                }
                RabbitMQSimpleSend sender = new RabbitMQSimpleSend(getCarToEvaluationDataForIndexQForTopic(topic), Car.EVALUATION_MODULE_ADDRESS);
                HashMap<String, String> data = transmittedDataRecord.getTransmittedDataOnTopicAndEmptyBuffer(topic);
                for(String message : data.values()){
                    sender.sendMessage(message);
                }
            });

            t.start();
        }
    }

    String getCarToControllerMessageQ(){
        return "C_TO_CO_" + this.carId;
    }

    String getControllerToCarMessageQ(){
        return "CO_TO_C_" + this.carId;
    }

    String getTopicQForCarFromTopicName(String topic){
        return topic + carId;
    }

    String getCarToEvaluationDataForIndexQForTopic(String topic){
        return MessageProps.CAR_TO_EVALUATION_INDEXING_PREFIX + "_" + carId + "_" + topic;
    }

    String getCarToRSUQForTopic(String topic){
        return topic + "_" + carId + "_TO_RSU";
    }

    String getRSUToCarInitQ(){
        return MessageQueue.rsuToCarPrefix + carId;
    }

}

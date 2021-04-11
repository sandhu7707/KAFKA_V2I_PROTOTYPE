package com.example.RSU;

import com.example.RSU.Consumer.ConsumedData;
import com.example.RSU.Model.MessageProps;
import com.example.RSU.Model.MessageQueue;
import com.example.RSU.Producer.ProducibleSensorData;
import com.example.RSU.RabbitMQ.RabbitMQSimpleReceive;
import com.example.RSU.RabbitMQ.RabbitMQSimpleSend;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.TimeoutException;

public class CarInstance {

    String carId;
    JSONArray topics;
    String RSU_TO_CAR_Q;
    boolean initFinished;
    public ProducibleSensorData sensorData;

    CarInstance(String carId, JSONArray topics) throws IOException, TimeoutException {
        this.carId = carId;
        this.topics = topics;
        this.RSU_TO_CAR_Q = "RSU_TO" + carId;
        this.initFinished = false;                                                                  //TODO: utilize or delete
        this.sensorData = new ProducibleSensorData();
        sendInitByRSU();

        startReceiversOn(topics);

        System.out.println("CarInstance initiated in RSU with carId: " + carId );
    }

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
                startReceiverOnTopic(topic);
            });

            t.start();
        }
    }

    private void startReceiverOnTopic(String topic) {
        RabbitMQSimpleReceive receiver = new RabbitMQSimpleReceive(getCarToRSUQForTopic(topic), RSU.RSU_ADDRESS);

        try {

            receiver.receive((s, delivery) -> {
                String message = new String(delivery.getBody(), "UTF-8");
                try {
                    JSONObject message_json = new JSONObject(message);

                    String message_id = (String) message_json.get(MessageProps.MESSAGE_ID);
                    message_json.put(MessageProps.RECEIVED_ON_RSU_AT, System.currentTimeMillis());

                    sensorData.updateRetransmissionDataOnTopic(topic, message_id, message_json.toString());

                } catch (JSONException | InterruptedException e) {
                    e.printStackTrace();
                }
            });
        } catch (IOException | TimeoutException e) {
            e.printStackTrace();
        }

    }

    private void sendInitByRSU() {
        String initQ = getRSUToCarInitQ();

        JSONObject initMessage_json = new JSONObject();

        initMessage_json.put(MessageProps.TYPE, MessageProps.INIT);
//        initMessage_json.put(MessageProps.CONSUMABLE_TOPICS, ConsumedData.consumedDataByTopics.keySet());

        RabbitMQSimpleSend sender = new RabbitMQSimpleSend(initQ, RSU.RSU_ADDRESS);
        sender.sendMessage(initMessage_json.toString());

        System.out.println("sent init message to car -> ");
        System.out.println(initMessage_json.toString());
    }


    String getRSUToCarInitQ(){
        return MessageQueue.rsuToCarPrefix + carId;
    }

    String getCarToRSUQForTopic(String topic){
        return topic + "_" + carId + "_TO_RSU";
    }
}

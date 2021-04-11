package com.example.Evaluation;

import com.example.Evaluation.Model.MessageProps;
import com.example.Evaluation.RabbitMQ.RabbitMQSimpleReceive;
import com.rabbitmq.client.DeliverCallback;
import com.rabbitmq.client.Delivery;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.TimeoutException;

public class EvaluationIndex {
    String carId;
    HashMap<String, HashMap<String, JSONObject>> index;                // <topic, <messageId, message>>
    JSONArray topics;

    public Boolean isAccessToAccessControlPermissible = true;
    public HashMap<String, Boolean> isAccessToEvaluationDataByTopicsPermissible = new HashMap<>();
    // TODO: set this up for multiThread access
    public HashMap<String, HashMap<String, String>> evaluationDataByTopics = new HashMap<>();

    public EvaluationIndex(String carId, JSONArray topics) {
        this.carId = carId;
        this.topics = topics;
        startReceiverFromCar();

    }

    public boolean checkAccessToEvaluationDataOnTopic(String topic) throws InterruptedException {

        if(isAccessToAccessControlPermissible) {
            isAccessToAccessControlPermissible = false;
            if(isAccessToEvaluationDataByTopicsPermissible.containsKey(topic)){
                isAccessToAccessControlPermissible = true;
                return isAccessToEvaluationDataByTopicsPermissible.get(topic);
            }
            else{
                isAccessToEvaluationDataByTopicsPermissible.put(topic, true);
                isAccessToAccessControlPermissible = true;
                return true;
            }
        }
        else{
            Thread.sleep(2);
            checkAccessToEvaluationDataOnTopic(topic);
        }

        return  false;
    }

    public void acquireAccessOnTopic(String topic){
        isAccessToEvaluationDataByTopicsPermissible.replace(topic, false);
    }

    public void looseAccessOnTopic(String topic){
        isAccessToEvaluationDataByTopicsPermissible.replace(topic, true);
    }

//    public void updateEvaluationDataOnTopic(String topic, HashMap<String, String> newData){
//        HashMap<String, String> data = EvaluationDataByTopics.get(topic);
//        data.addAll(newData);
//        EvaluationDataByTopics.replace(topic, data);
////        System.out.print("new data added -> ");
////        System.out.println(newData);
//    }

    public void updateEvaluationDataOnTopic(String topic, HashMap<String, String> newData) throws InterruptedException {
        HashMap<String, String> data;

        if(!evaluationDataByTopics.containsKey(topic)){
            evaluationDataByTopics.put(topic, new HashMap<>());
        }

        data = evaluationDataByTopics.get(topic);

        Thread t = new Thread(()-> {
            for(String key: newData.keySet()){
                if(data.containsKey(key)){
                    data.replace(key, newData.get(key));
                }
                else{
                    data.put(key, newData.get(key));
                }
                System.out.println("updates in evaluation data -> " + newData.get(key));
            }

            while(true){
                try {
                    if (checkAccessToEvaluationDataOnTopic(topic)) break;
                    Thread.sleep(2);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            acquireAccessOnTopic(topic);
            evaluationDataByTopics.replace(topic, data);
            looseAccessOnTopic(topic);

        });
        t.start();
    }

    public HashMap<String, String> getEvaluationDataOnTopicAndEmptyBuffer(String topic){
        HashMap<String, String> data;
        if(!evaluationDataByTopics.containsKey(topic)){
            evaluationDataByTopics.put(topic, new HashMap<>());
        }
        data = evaluationDataByTopics.get(topic);

        Thread t = new Thread(()-> {

            while(true){
                try {
                    if (checkAccessToEvaluationDataOnTopic(topic)) break;
                    Thread.sleep(2);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            acquireAccessOnTopic(topic);
            evaluationDataByTopics.replace(topic,new HashMap<>());
            looseAccessOnTopic(topic);

        });
        t.start();
        System.out.println("fetched evaluation data -> " + data);
        return data;
    }

    public void startReceiverFromCar(){

        for(int i = 0; i<topics.length(); i++){

            int finalI = i;
            Thread t = new Thread(()->{
                RabbitMQSimpleReceive receiver = null;
                try {
                    receiver = new RabbitMQSimpleReceive(getCarToEvaluationDataForIndexQForTopic((String) topics.get(finalI)));
                } catch (JSONException e) {
                    e.printStackTrace();
                }
                try {
                    receiver.receive(new DeliverCallback() {
                        @Override
                        public void handle(String s, Delivery delivery) throws IOException {
                            String message = new String(delivery.getBody(), "UTF-8");
                            try {
                                System.out.println("received message for evaluation index -> " + message);

                                // save evaluation index as csv file when car position > 2000

                                JSONObject message_json = new JSONObject(message);



                            } catch (JSONException e) {
                                e.printStackTrace();
                            }
                        }
                    });
                } catch (IOException | TimeoutException e) {
                    e.printStackTrace();
                }
            });

            t.start();
        }
    }

    public void startReceiverFromRSU(){
        Thread t = new Thread(()->{
            RabbitMQSimpleReceive receiver = new RabbitMQSimpleReceive(getRSUToEvaluationDataForIndexQ());
            try {
                receiver.receive(new DeliverCallback() {
                    @Override
                    public void handle(String s, Delivery delivery) throws IOException {
                        String message = new String(delivery.getBody(), "UTF-8");
                        try {

                            JSONObject message_json = new JSONObject(message);



                        } catch (JSONException e) {
                            e.printStackTrace();
                        }
                    }
                });
            } catch (IOException | TimeoutException e) {
                e.printStackTrace();
            }
        });
    }

    String getCarToEvaluationDataForIndexQForTopic(String topic){
        return MessageProps.CAR_TO_EVALUATION_INDEXING_PREFIX + "_" + carId + "_" + topic;
    }

    String getRSUToEvaluationDataForIndexQ(){
        return MessageProps.RSU_TO_EVALUATION_INDEXING_PREFIX + carId;
    }

}

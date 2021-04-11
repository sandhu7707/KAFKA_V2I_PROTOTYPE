package com.example.RSU.Producer;

import org.json.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;


// check -> available ? -> acquire -> update (from local HashMap) -> loose
// check -> not available ? -> update local HashMap

public class ProducibleSensorData {

    public Boolean isAccessToAccessControlPermissible = true;
    public HashMap<String, Boolean> isAccessToSensorDataByTopicsPermissible = new HashMap<>();
    // TODO: set this up for multiThread access
    public HashMap<String, HashMap<String, String>> sensorDataByTopics = new HashMap<>();

    public boolean checkAccessToSensorDataOnTopic(String topic) throws InterruptedException {

        if(isAccessToAccessControlPermissible) {
            isAccessToAccessControlPermissible = false;
            if(isAccessToSensorDataByTopicsPermissible.containsKey(topic)){
                isAccessToAccessControlPermissible = true;
                return isAccessToSensorDataByTopicsPermissible.get(topic);
            }
            else{
                isAccessToSensorDataByTopicsPermissible.put(topic, true);
                isAccessToAccessControlPermissible = true;
                return true;
            }
        }
        else{
            Thread.sleep(2);
            checkAccessToSensorDataOnTopic(topic);
        }

        return  false;
    }

    public void acquireAccessOnTopic(String topic){
        isAccessToSensorDataByTopicsPermissible.replace(topic, false);
    }

    public void looseAccessOnTopic(String topic){
        isAccessToSensorDataByTopicsPermissible.replace(topic, true);
    }

    public void updateRetransmissionDataOnTopic(String topic, String message_id, String message) throws InterruptedException {

        HashMap<String, String> data;
        if(!sensorDataByTopics.containsKey(topic)){
            sensorDataByTopics.put(topic, new HashMap<>());
        }

        data = sensorDataByTopics.get(topic);

        Thread t = new Thread(()-> {

            if(data.containsKey(message_id)){
                data.replace(message_id, message);
            }
            else {
                data.put(message_id, message);
            }

//            System.out.println("added data ->" + message);

            while(true){
                try {
                    if (checkAccessToSensorDataOnTopic(topic)) break;
                    Thread.sleep(2);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            acquireAccessOnTopic(topic);
            sensorDataByTopics.replace(topic,data);
            looseAccessOnTopic(topic);

        });
        t.start();
    }

//    public void updateSensorDataOnTopic(String topic, ArrayList<String> newData){
//        ArrayList<String> data = sensorDataByTopics.get(topic);
//        data.addAll(newData);
//        sensorDataByTopics.replace(topic, data);
////        System.out.print("new data added -> ");
////        System.out.println(newData);
//    }
//

    public HashMap<String, String> getSensorDataOnTopicAndEmptyBuffer(String topic){
        HashMap<String, String> data;
        if(!sensorDataByTopics.containsKey(topic)){
            sensorDataByTopics.put(topic, new HashMap<>());
        }
        data = sensorDataByTopics.get(topic);

        Thread t = new Thread(()-> {

            while(true){
                try {
                    if (checkAccessToSensorDataOnTopic(topic)) break;
                    Thread.sleep(2);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            acquireAccessOnTopic(topic);
            sensorDataByTopics.replace(topic,new HashMap<>());
            looseAccessOnTopic(topic);

        });

        t.start();

//        System.out.println("fetched sensor data -> " + data);

        return data;
    }

}

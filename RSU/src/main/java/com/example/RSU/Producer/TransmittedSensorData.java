package com.example.RSU.Producer;

import java.util.HashMap;

public class TransmittedSensorData {

    public Boolean isAccessToAccessControlPermissible = true;
    public HashMap<String, Boolean> isAccessToTransmittedDataByTopicsPermissible = new HashMap<>();
    // TODO: set this up for multiThread access
    public HashMap<String, HashMap<String, String>> transmittedDataByTopics = new HashMap<>();

    public boolean checkAccessToTransmittedDataOnTopic(String topic) throws InterruptedException {

        if(isAccessToAccessControlPermissible) {
            isAccessToAccessControlPermissible = false;
            if(isAccessToTransmittedDataByTopicsPermissible.containsKey(topic)){
                isAccessToAccessControlPermissible = true;
                return isAccessToTransmittedDataByTopicsPermissible.get(topic);
            }
            else{
                isAccessToTransmittedDataByTopicsPermissible.put(topic, true);
                isAccessToAccessControlPermissible = true;
                return true;
            }
        }
        else{
            Thread.sleep(2);
            checkAccessToTransmittedDataOnTopic(topic);
        }

        return  false;
    }

    public void acquireAccessOnTopic(String topic){
        isAccessToTransmittedDataByTopicsPermissible.replace(topic, false);
    }

    public void looseAccessOnTopic(String topic){
        isAccessToTransmittedDataByTopicsPermissible.replace(topic, true);
    }

//    public void updateTransmittedDataOnTopic(String topic, HashMap<String, String> newData){
//        HashMap<String, String> data = transmittedDataByTopics.get(topic);
//        data.addAll(newData);
//        transmittedDataByTopics.replace(topic, data);
////        System.out.print("new data added -> ");
////        System.out.println(newData);
//    }

    public void updateTransmittedDataOnTopic(String topic,String message_id, String message) throws InterruptedException {
        HashMap<String, String> data;

        if(!transmittedDataByTopics.containsKey(topic)){
            transmittedDataByTopics.put(topic, new HashMap<>());
        }

        data = transmittedDataByTopics.get(topic);

        Thread t = new Thread(()-> {
                if(data.containsKey(message_id)){
                    data.replace(message_id, message);
                }
                else{
                    data.put(message_id, message);
                }
                System.out.println("updates in transmitted data -> " + message);


            while(true){
                try {
                    if (checkAccessToTransmittedDataOnTopic(topic)) break;
                    Thread.sleep(2);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            acquireAccessOnTopic(topic);
            transmittedDataByTopics.replace(topic, data);
            looseAccessOnTopic(topic);

        });
        t.start();
    }

    public HashMap<String, String> getTransmittedDataOnTopicAndEmptyBuffer(String topic){
        HashMap<String, String> data;
        if(!transmittedDataByTopics.containsKey(topic)){
            transmittedDataByTopics.put(topic, new HashMap<>());
        }
        data = transmittedDataByTopics.get(topic);

        Thread t = new Thread(()-> {

            while(true){
                try {
                    if (checkAccessToTransmittedDataOnTopic(topic)) break;
                    Thread.sleep(2);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            acquireAccessOnTopic(topic);
            transmittedDataByTopics.replace(topic,new HashMap<>());
            looseAccessOnTopic(topic);

        });
        t.start();
//        System.out.println("fetched retransmission data -> " + data);
        return data;
    }

}

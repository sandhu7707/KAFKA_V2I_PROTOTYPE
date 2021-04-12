package com.example.Car;

import java.util.ArrayList;
import java.util.HashMap;

public class RetransmissionsByTopics {
    public Boolean isAccessToAccessControlPermissible = true;
    public HashMap<String, Boolean> isAccessToRetransmissionDataByTopicsPermissible = new HashMap<>();
    // TODO: set this up for multiThread access
    public HashMap<String, ArrayList<String>> retransmissionDataByTopics = new HashMap<>();

    public boolean checkAccessToRetransmissionDataOnTopic(String topic) throws InterruptedException {

        if(isAccessToAccessControlPermissible) {
            isAccessToAccessControlPermissible = false;
            if(isAccessToRetransmissionDataByTopicsPermissible.containsKey(topic)){
                isAccessToAccessControlPermissible = true;
                return isAccessToRetransmissionDataByTopicsPermissible.get(topic);
            }
            else{
                isAccessToRetransmissionDataByTopicsPermissible.put(topic, true);
                isAccessToAccessControlPermissible = true;
                return true;
            }
        }
        else{
            Thread.sleep(2);
            checkAccessToRetransmissionDataOnTopic(topic);
        }

        return  false;
    }

    public void acquireAccessOnTopic(String topic){
        isAccessToRetransmissionDataByTopicsPermissible.replace(topic, false);
    }

    public void looseAccessOnTopic(String topic){
        isAccessToRetransmissionDataByTopicsPermissible.replace(topic, true);
    }

//    public void updateRetransmissionDataOnTopic(String topic, HashMap<String, String> newData){
//        HashMap<String, String> data = RetransmissionDataByTopics.get(topic);
//        data.addAll(newData);
//        RetransmissionDataByTopics.replace(topic, data);
////        System.out.print("new data added -> ");
////        System.out.println(newData);
//    }

    public void updateRetransmissionDataOnTopic(String topic, ArrayList<String> newData) throws InterruptedException {
        ArrayList<String> data;
        if(!retransmissionDataByTopics.containsKey(topic)){
            retransmissionDataByTopics.put(topic, new ArrayList<>());
        }
        data = retransmissionDataByTopics.get(topic);
        Thread t = new Thread(()-> {

            while(true){
                try {
                    if (checkAccessToRetransmissionDataOnTopic(topic)) break;
                    Thread.sleep(2);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            data.addAll(newData);
            acquireAccessOnTopic(topic);
            retransmissionDataByTopics.replace(topic,data);
            looseAccessOnTopic(topic);

        });
        t.start();
    }

    public ArrayList<String> getRetransmissionDataOnTopicAndEmptyBuffer(String topic){
        ArrayList<String> data;
        if(!retransmissionDataByTopics.containsKey(topic)){
            retransmissionDataByTopics.put(topic, new ArrayList<>());
        }
        data = retransmissionDataByTopics.get(topic);

        Thread t = new Thread(()-> {

            while(true){
                try {
                    if (checkAccessToRetransmissionDataOnTopic(topic)) break;
                    Thread.sleep(2);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            acquireAccessOnTopic(topic);
            retransmissionDataByTopics.replace(topic,new ArrayList<>());
            looseAccessOnTopic(topic);

        });
        t.start();
//        System.out.println("fetched retransmission data -> " + data);
        return data;
    }
}

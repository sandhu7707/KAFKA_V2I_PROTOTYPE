package com.example.RSU.Consumer;

import java.util.ArrayList;
import java.util.HashMap;

public class ConsumedData {

    public static HashMap<String, HashMap<Long, String>> consumedDataByTopics = new HashMap<>();
    static Boolean isAccessToAccessControlPermissible = true;
    static HashMap<String, Boolean> isAccessToConsumedDataByTopicsPermissible = new HashMap<>();

    public static boolean checkAccessToConsumedDataOnTopic(String topic) throws InterruptedException {

        if(isAccessToAccessControlPermissible) {
            isAccessToAccessControlPermissible = false;
            if(isAccessToConsumedDataByTopicsPermissible.containsKey(topic)){
                isAccessToAccessControlPermissible = true;
                return isAccessToConsumedDataByTopicsPermissible.get(topic);
            }
            else{
                isAccessToAccessControlPermissible = true;
                isAccessToConsumedDataByTopicsPermissible.put(topic, true);
                return true;
            }
        }
        else{
            Thread.sleep(2);
            checkAccessToConsumedDataOnTopic(topic);
        }

        return  false;
    }

    public static void acquireAccessOnTopic(String topic){
        isAccessToConsumedDataByTopicsPermissible.replace(topic, false);
    }

    public static void looseAccessOnTopic(String topic){
        isAccessToConsumedDataByTopicsPermissible.replace(topic, true);
    }

//    public void updateSensorDataOnTopic(String topic, ArrayList<String> newData){
//        ArrayList<String> data = sensorDataByTopics.get(topic);
//        data.addAll(newData);
//        sensorDataByTopics.replace(topic, data);
//        System.out.print("new data added -> ");
//        System.out.println(newData);
//    }
}

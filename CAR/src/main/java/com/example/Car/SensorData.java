package com.example.Car;

import java.util.ArrayList;
import java.util.HashMap;


// check -> available ? -> acquire -> update (from local HashMap) -> loose
// check -> not available ? -> update local HashMap

// only conflict possible in Car is that receiver from evaluation and producer to RSU writes at same time, it won't occur much often.. this class is developed as a buffer for RSU and can be used here without any loss in performance
public class SensorData {

    public Boolean isAccessToAccessControlPermissible = true;
    public HashMap<String, Boolean> isAccessToSensorDataByTopicsPermissible = new HashMap<>();
    // TODO: set this up for multiThread access
    public HashMap<String, ArrayList<String>> sensorDataByTopics = new HashMap<>();

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

    public void updateSensorDataOnTopic(String topic, ArrayList<String> newData){
        ArrayList<String> data = sensorDataByTopics.get(topic);
        data.addAll(newData);
        sensorDataByTopics.replace(topic, data);
//        System.out.print("new data added -> ");
//        System.out.println(newData);
    }

}

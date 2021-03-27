import RabbitMQ.RabbitMQReceive;
import RabbitMQ.RabbitMQSend;
import com.rabbitmq.client.DeliverCallback;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.concurrent.TimeoutException;

public class RSU {

    static Boolean isAccessToAccessControlPermissible = true;
    static HashMap<String, Boolean> isAccessToSensorDataByTopicsPermissible = new HashMap<>();
    // TODO: set this up for multiThread access
    static HashMap<String, ArrayList<String>> sensorDataByTopics = new HashMap<>();

    public static boolean checkAccessToSensorDataOnTopic(String topic) throws InterruptedException {

        if(isAccessToAccessControlPermissible) {
            isAccessToAccessControlPermissible = false;
            if(isAccessToSensorDataByTopicsPermissible.containsKey(topic)){
                isAccessToAccessControlPermissible = true;
                return isAccessToSensorDataByTopicsPermissible.get(topic);
            }
            else{
                isAccessToAccessControlPermissible = true;
                isAccessToSensorDataByTopicsPermissible.put(topic, true);
                return true;
            }
        }
        else{
            Thread.sleep(2);
            checkAccessToSensorDataOnTopic(topic);
        }

        return  false;
    }

    public static void acquireAccessOnTopic(String topic){
        isAccessToSensorDataByTopicsPermissible.replace(topic, false);
    }

    public static void looseAccessOnTopic(String topic){
        isAccessToSensorDataByTopicsPermissible.replace(topic, true);
    }

    //TODO: see if making this return a boolean is worth it!
    public static void updateSensorDataOnTopic(String topic, ArrayList<String> newData){
        isAccessToSensorDataByTopicsPermissible.replace(topic, false);
        acquireAccessOnTopic(topic);
        ArrayList<String> data = sensorDataByTopics.get(topic);
        data.addAll(newData);
        sensorDataByTopics.replace(topic, data);
        looseAccessOnTopic(topic);
        isAccessToSensorDataByTopicsPermissible.replace(topic, true);
    }

    //TODO: add scheduled producers here that send the sensorData stuff....
    public static void main(String[] args) throws IOException, TimeoutException {

        Thread t = new Thread(()-> {
            try {
                Consumer consumer = new Consumer();
                consumer.startConsumerFor("POSE_LOCALIZED");
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        });
        t.start();


        startReceiver();
    }

    static void startReceiver() throws IOException, TimeoutException {

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            JSONObject message_json = new JSONObject(message);

            switch(message_json.get("TYPE").toString()){
                case "INIT":
                    handleInit();
                    try {
                        startReceiversOnTopicsAndStartProducers((JSONArray) message_json.get("SENSOR_TOPICS"));
                    } catch (TimeoutException e) {
                        e.printStackTrace();
                    }
                    break;
            }

//            System.out.println(message_json.get("TYPE"));

//            System.out.println(" [x] Received '" + message + "'");
        };

        RabbitMQReceive receiver = new RabbitMQReceive(initToRSU);
        receiver.receive(deliverCallback);
    }

    private static void startReceiversOnTopicsAndStartProducers(JSONArray sensor_topics) throws IOException, TimeoutException {
        for(int i = 0; i < sensor_topics.length(); i++ ){
            CarSensorDataReceiver carSensorDataReceiver = new CarSensorDataReceiver((String) sensor_topics.get(i));
            carSensorDataReceiver.startReceiving();
        }

        Thread t = new Thread(()-> {
            try {
                Producer producer = new Producer();
                producer.initializeProducer();
                producer.scheduledStarter();
            } catch (InterruptedException | JSONException e) {
                e.printStackTrace();
            }
        });

        t.start();

    }

    private static void handleInit() {
        RabbitMQSend initSender = new RabbitMQSend(initToCar);
        JSONObject data_JSON = new JSONObject();
        data_JSON.put("DATA", "");
        data_JSON.put("TYPE", "INIT");
        data_JSON.put("CONSUMABLE_TOPICS", consumableTopics);
        initSender.sendMessage(data_JSON.toString());
    }

    static JSONArray consumableTopics = new JSONArray(Consumer.consumableDataByTopics.keySet());
    static String initToRSU = "INIT-TO-RSU";
    static String initToCar = "INIT-TO-CAR";

}

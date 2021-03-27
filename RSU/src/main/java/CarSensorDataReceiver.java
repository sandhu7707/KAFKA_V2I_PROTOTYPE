import RabbitMQ.RabbitMQReceive;
import com.rabbitmq.client.DeliverCallback;
import org.json.JSONObject;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.concurrent.TimeoutException;

public class CarSensorDataReceiver {
    String qName;
    HashMap<String, ArrayList<String>> newDataOnTopic = new HashMap<>();

    CarSensorDataReceiver(String qName){
        this.qName = qName;
    }

    void startReceiving() throws IOException, TimeoutException {
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            System.out.println(message);
            try {
                handleMessage(message);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        };

        RabbitMQReceive receiver = new RabbitMQReceive(qName);
        receiver.receive(deliverCallback);

    }

    void handleMessage(String message) throws InterruptedException {
        JSONObject message_json = new JSONObject(message);
        String topic = (String) message_json.get("TOPIC");

        //TODO: check if the parallel access works..

        if(RSU.sensorDataByTopics.containsKey(topic)){
            ArrayList<String> newData = newDataOnTopic.get(topic);

            if(RSU.checkAccessToSensorDataOnTopic(topic)){
                newData.add(message);
                RSU.updateSensorDataOnTopic(topic, newData);
                newData = new ArrayList<>();
            }
            else{
                newData.add(message);
            }

            newDataOnTopic.replace(topic, newData);
        }
        else{
            RSU.sensorDataByTopics.put(topic, new ArrayList<String>(Collections.singleton(message)));
            newDataOnTopic.put(topic, new ArrayList<>());
        }
    }
}

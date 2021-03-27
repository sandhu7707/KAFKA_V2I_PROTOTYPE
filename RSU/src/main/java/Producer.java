import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.Properties;
import java.util.Set;

public class Producer {

    private KafkaProducer<String, String> producer;
    private final String[] broker_addrs = {"localhost:9092"};
    private ArrayList<String> topicsBeingProduced = new ArrayList<>();
    private long tickTime = 5;

    void scheduledStarter() throws InterruptedException, JSONException {

        Set<String> topicsWithDataAvailable;
        if(RSU.isAccessToAccessControlPermissible){
            RSU.isAccessToAccessControlPermissible = false;
            topicsWithDataAvailable = RSU.isAccessToSensorDataByTopicsPermissible.keySet();
            RSU.isAccessToAccessControlPermissible = true;
            for(String topic: topicsWithDataAvailable){
                if (!topicsBeingProduced.contains(topic)){
                    topicsBeingProduced.add(topic);
                    Thread t = new Thread(() -> {
                        try {
                            startProducerOnTopic(topic);
                        } catch (JSONException | InterruptedException e) {
                            e.printStackTrace();
                        }
                    });

                    t.start();
                }
            }
            Thread.sleep(tickTime*1000);
            scheduledStarter();
        }
        else {
            Thread.sleep(5);
            scheduledStarter();
        }

    }

    void initializeProducer() {
        //starting producer
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 1);
        props.put("linger.ms", 1);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // todo: try a json or string array serializing for csvs
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<String, String>(props);

    }

    void startProducerOnTopic(String topic) throws JSONException, InterruptedException {
        if(RSU.checkAccessToSensorDataOnTopic(topic)){
            RSU.acquireAccessOnTopic(topic);
            ArrayList<String> dataToBeProduced = RSU.sensorDataByTopics.get(topic);
            RSU.sensorDataByTopics.replace(topic, new ArrayList<>());
            RSU.looseAccessOnTopic(topic);

            for(String message: dataToBeProduced){
                JSONObject message_json = new JSONObject(message);
                message_json.put("PRODUCER_TIMESTAMP", System.currentTimeMillis());
                System.out.println("producing key: " + message_json.get("TYPE") + ", message: " + message_json.toString());
                producer.send(new ProducerRecord<>(topic, (String) message_json.get("TYPE"), message_json.toString()));
            }

            topicsBeingProduced.remove(topic);
        }

    }
}
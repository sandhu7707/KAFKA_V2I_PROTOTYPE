package com.example.RSU.Producer;

import com.example.RSU.CarInstance;
import com.example.RSU.Model.MessageProps;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.HashMap;
import java.util.Properties;

public class Producer {
    //produce to kafka and to evaluation module !
    //employ transmitted buffer -> ?

    TransmittedSensorData transmittedSensorData;
    CarInstance carInstance;
    JSONArray topics;

    public Producer(CarInstance carInstance, JSONArray topics){
        this.transmittedSensorData = new TransmittedSensorData();
        this.carInstance = carInstance;
        this.topics = topics;
        startKafkaProducer();
    }

    private void startKafkaProducer() {

        for(int i =0; i< topics.length(); i++) {
            int finalI = i;
            Thread t = new Thread(()-> {
                try {
                    startProducerOnTopic((String) topics.get(finalI));
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            });
            t.start();
        }
    }

    private void startProducerOnTopic(String topic) throws InterruptedException {

        //starting producer
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 1);
        props.put("linger.ms", 1);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // todo: try a json or string array serializing for csvs
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

        while(true) {
            HashMap<String, String> data = carInstance.sensorData.getSensorDataOnTopicAndEmptyBuffer(topic);

            for(String key: data.keySet()){
                String message = data.get(key);
                JSONObject message_json = new JSONObject(message);

                message_json.put(MessageProps.SENT_FROM_RSU_AT, System.currentTimeMillis());
                message = message_json.toString();

//                System.out.println("starting producer on topic -> "+ topic);
                transmittedSensorData.updateTransmittedDataOnTopic(topic, (String) message_json.get(MessageProps.MESSAGE_ID), message);
                System.out.println("producing on kafka broker -> " + message);
                producer.send(new ProducerRecord<>(topic, (String) message_json.get(MessageProps.MESSAGE_ID), message_json.toString()));

            }
        }
    }

}

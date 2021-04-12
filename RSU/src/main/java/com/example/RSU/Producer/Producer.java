package com.example.RSU.Producer;

import com.example.RSU.CarInstance;
import com.example.RSU.Model.MessageProps;
import com.example.RSU.RabbitMQ.RabbitMQSimpleSend;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.HashMap;
import java.util.Properties;

public class Producer {
    private static final String EVALUATION_MODULE_ADDRESS = "localhost";
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

            t = new Thread(()-> {
                try {
                    sendTransmittedDataToEvaluationOnTopic((String) topics.get(finalI));
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
            HashMap<String, String> itr = carInstance.sensorData.getSensorDataOnTopicAndEmptyBuffer(topic);

            HashMap<String, String> data = new HashMap<>(itr);

            for(String key: data.keySet()){
                String message = data.get(key);
                JSONObject message_json = new JSONObject(message);

                message_json.put(MessageProps.SENT_FROM_RSU_AT, System.currentTimeMillis());
                message_json.put(MessageProps.CAR_ID, carInstance.carId);
                message = message_json.toString();

//                System.out.println("starting producer on topic -> "+ topic);
                transmittedSensorData.updateTransmittedDataOnTopic(topic, (String) message_json.get(MessageProps.MESSAGE_ID), message);
//                System.out.println("producing on kafka broker -> " + message);
                producer.send(new ProducerRecord<>(topic, (String) message_json.get(MessageProps.MESSAGE_ID), message_json.toString()));

            }
//            Thread.sleep(3000);
        }
    }

    private void sendTransmittedDataToEvaluationOnTopic(String topic) throws InterruptedException {
        RabbitMQSimpleSend sender = new RabbitMQSimpleSend(getRSUToEvaluationDataForIndexQForTopic(topic), EVALUATION_MODULE_ADDRESS);

        while(true){
            HashMap<String, String> itr = transmittedSensorData.getTransmittedDataOnTopicAndEmptyBuffer(topic);
            HashMap<String, String> data = new HashMap<>(itr);
            for(String message: data.values()){
//                System.out.println("sending to evaluation -> " + message);
                sender.sendMessage(message);

            }
            Thread.sleep(3000);
        }

    }

    String getRSUToEvaluationDataForIndexQForTopic(String topic){
        return MessageProps.RSU_TO_EVALUATION_INDEXING_PREFIX + "_" + carInstance.carId + "_" + topic;
    }

}

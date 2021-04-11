package com.example.RSU;

import com.example.RSU.Model.MessageProps;
import com.example.RSU.Model.MessageQueue;
import com.example.RSU.Producer.Producer;
import com.example.RSU.RabbitMQ.RabbitMQSimpleReceive;
import com.rabbitmq.client.DeliverCallback;
import com.rabbitmq.client.Delivery;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class RSU {

    public static String RSU_ADDRESS = "localhost";

    public static void main(String[] args){

        Thread t = new Thread(()->{
            try {
                startListenerForCars();
            } catch (IOException | TimeoutException e) {
                e.printStackTrace();
            }
        });

        t.start();
    }

    private static void startListenerForCars() throws IOException, TimeoutException {
        RabbitMQSimpleReceive receiver = new RabbitMQSimpleReceive(MessageQueue.carToRSUInitQ, RSU_ADDRESS);
        receiver.receive(new DeliverCallback() {
            @Override
            public void handle(String s, Delivery delivery) throws IOException {
                String message = new String(delivery.getBody(), "UTF-8");
                JSONObject message_json = new JSONObject(message);

                if(((String) message_json.get(MessageProps.TYPE)).equals(MessageProps.INIT)){
                    JSONArray topics = (JSONArray) message_json.get(MessageProps.TOPICS);
                    String carId = (String) message_json.get(MessageProps.CAR_ID);
                    try {
                        CarInstance carInstance = new CarInstance(carId, topics);               //need to figure a way when to stop carInstance...!
                        Producer producer = new Producer(carInstance, topics);
                    } catch (TimeoutException e) {
                        e.printStackTrace();
                    }

                }
            }
        });
    }

}

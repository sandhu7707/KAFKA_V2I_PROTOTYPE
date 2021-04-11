package com.example.Car;

import com.example.Car.Model.MessageProps;
import com.example.Car.Model.MessageQueue;
import com.example.Car.RabbitMQ.RabbitMQSimpleReceive;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import java.io.IOException;
import java.util.concurrent.TimeoutException;


public class StartCarInstance {

    public static void main(String[] args) {

        Thread listenerForEvaluationController = new Thread(()-> {
            RabbitMQSimpleReceive listener = new RabbitMQSimpleReceive(MessageQueue.carInitQ, Car.EVALUATION_MODULE_ADDRESS);
            try {
                listener.receive((s, delivery) -> {
                    String message = new String(delivery.getBody(), "UTF-8");
                    try {

                        JSONObject message_json = new JSONObject(message);

                        if(((String) message_json.get(MessageProps.TYPE)).equals(MessageProps.INIT)){

                            Thread t = new Thread(() -> {
                                try {

                                    JSONArray topics = (JSONArray) message_json.get(MessageProps.TOPICS);

                                    CarProps carProps = new CarProps((String) message_json.get(MessageProps.MODE),
                                            (String) message_json.get(MessageProps.CAR_ID),
                                            (int) message_json.get(MessageProps.PRIORITY), topics);

                                    new Car(carProps, topics);

                                } catch (JSONException | TimeoutException | IOException | InterruptedException e) {
                                    e.printStackTrace();
                                }
                            });

                            t.start();

                            System.out.print("In StartCarInstance.main, executed Car.StartCarInstances -> ");
                            System.out.println(message);

                        }

                    } catch (JSONException e) {
                        e.printStackTrace();
                    }
                });
            } catch (IOException | TimeoutException e) {
                e.printStackTrace();
            }
        });

        listenerForEvaluationController.start();
    }

}

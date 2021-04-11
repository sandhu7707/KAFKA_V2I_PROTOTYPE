package com.example.RSU.RabbitMQ;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class RabbitMQSimpleReceive {

    String receiverAddress;
    String qName;

    public RabbitMQSimpleReceive(String qName, String receiverAddress){
        this.qName = qName;
        this.receiverAddress = receiverAddress;
    }

    public void receive(DeliverCallback deliverCallback) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(receiverAddress);
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.queueDeclare(qName, true, false, false, null);
//        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        channel.basicConsume(qName, true, deliverCallback, consumerTag -> { });
        //by default we have autoAck enabled in this class..
    }


}
package RabbitMQ;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;
import java.io.IOException;
import java.util.concurrent.TimeoutException;


public class RabbitMQSend {

    private final String qName;
    private String receiverAddress;
//    private int receiverPort;

    public RabbitMQSend(String qName, String receiverAddress) {
        this.qName = qName;
        this.receiverAddress = receiverAddress;
    }

    private void setReceiverAddress(String receiverAddress) {
        String[] addressInSplits = receiverAddress.split(":");
        this.receiverAddress = addressInSplits[0];
//        this.receiverPort = parseInt(addressInSplits[1]);
    }

    public void updateReceiverAddress(String receiverAddress){
        this.receiverAddress = receiverAddress;
    }

    public void sendMessage(String message){
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(receiverAddress);
//        factory.setPort(receiverPort);
        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {
            channel.queueDeclare(qName, true, false, false, null);

            channel.basicPublish("", qName,
                    MessageProperties.PERSISTENT_TEXT_PLAIN,
                    message.getBytes("UTF-8"));
//            System.out.println(" [x] Sent '" + message + "'");
        } catch (TimeoutException | IOException e) {
            e.printStackTrace();
        }
    }

}
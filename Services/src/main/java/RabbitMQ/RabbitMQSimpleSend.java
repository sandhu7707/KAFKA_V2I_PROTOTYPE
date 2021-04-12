package RabbitMQ;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class RabbitMQSimpleSend {

    private String qName;
    private String receiverAddress;
//    private int receiverPort;

    public RabbitMQSimpleSend(String qName, String receiverAddress) {
        this.qName = qName;
        this.setReceiverAddress(receiverAddress);
    }

    private void setReceiverAddress(String receiverAddress) {
        String[] addressInSplits = receiverAddress.split(":");
        this.receiverAddress = addressInSplits[0];
//        this.receiverPort = parseInt(addressInSplits[1]);
    }

    public void updateReceiverAddress(String receiverAddress){
        setReceiverAddress(receiverAddress);
    }

    public void sendMessage(String message){
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(receiverAddress);
        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {
            channel.queueDeclare(qName, true, false, false, null);

            channel.basicPublish("", qName,
                    MessageProperties.PERSISTENT_TEXT_PLAIN,
                    message.getBytes("UTF-8"));

        } catch (TimeoutException | IOException e) {
            e.printStackTrace();
        }
    }

}
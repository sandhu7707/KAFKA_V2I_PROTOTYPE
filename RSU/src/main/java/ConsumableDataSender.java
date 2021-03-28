import RabbitMQ.RabbitMQReceive;
import RabbitMQ.RabbitMQSend;
import com.rabbitmq.client.DeliverCallback;
import org.json.JSONObject;

import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.TimeoutException;

public class ConsumableDataSender {

    String qName;
    RabbitMQReceive receiver;
    RabbitMQSend sender;

    ConsumableDataSender(String qName){
        this.qName = qName;
        receiver = new RabbitMQReceive(qName);
        sender = new RabbitMQSend(qName);
    }

    void initiateSenderByStartingListener() throws IOException, TimeoutException {
        receiver.receive(getCallbackListener());
    }

    DeliverCallback getCallbackListener(){
     return (s, delivery) -> {
        String message = new String(delivery.getBody());
         System.out.println(message);
         JSONObject message_json = new JSONObject(message);
         if(((String)message_json.get("TYPE")).equals("INIT-CONSUME")){
             System.out.println("starting to send..");
             long offset = (int) message_json.get("OFFSET");
             System.out.println("starting to send..");
             try {
                 System.out.println("starting to send..");
                 startSenderFromOffset(offset);
             } catch (InterruptedException e) {
                 e.printStackTrace();
             }
         }
     };
    }

    void startSenderFromOffset(long offset) throws InterruptedException {
        HashMap<Long, String> data = getDataFromOffset(offset);
        if(data != null) {
            for (long key : data.keySet()) {
                JSONObject message_json = new JSONObject();
                message_json.put("TYPE", "DATA");
                message_json.put("OFFSET", key);
                message_json.put("DATA", data.get(key));
                offset = key;
                sender.sendMessage(message_json.toString());
                System.out.println(message_json.toString());
            }
        }
        Thread.sleep(3*1000);
        startSenderFromOffset(offset);

    }

    HashMap<Long, String> getDataFromOffset(long offset) throws InterruptedException {
        System.out.println(qName);
        System.out.println(Consumer.consumableDataByTopics.get(qName));

        HashMap<Long, String> dataOnTopic = Consumer.consumableDataByTopics.get(qName);
        if (dataOnTopic.containsKey(offset)) {
            HashMap<Long, String> ret = new HashMap<>();
            System.out.println("starting with offset ---->");
            System.out.println(offset);
            for (long key : dataOnTopic.keySet()) {
                if (key >= offset) {
                    ret.put(key, dataOnTopic.get(key));
                }
            }
            return ret;
        } else {
            return dataOnTopic;
        }
    }
}

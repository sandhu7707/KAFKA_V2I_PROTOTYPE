import RabbitMQ.RabbitMQReceive;
import RabbitMQ.RabbitMQSend;
import com.rabbitmq.client.DeliverCallback;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class ConsumableDataReceiver {

    String qName;
    int offset;
    String RSUAddress;
    RabbitMQSend sender;
    RabbitMQReceive rabbitMQReceive;
    File f;
    FileWriter writer;

    ConsumableDataReceiver(String qName, String RSUAddress) throws IOException {
        this.qName = qName;
        this.RSUAddress = RSUAddress;
        sender = new RabbitMQSend(qName, RSUAddress);
        f = new File("Car/src/main/resources/generated/" + qName + ".csv");
        writer = new FileWriter("Car/src/main/resources/generated/" + qName + ".csv");
    }

    void updateRSUAddress(String RSUAddress){
        this.RSUAddress = RSUAddress;
        sender.updateReceiverAddress(RSUAddress);
    }

    String getInitConsumeMessage() throws JSONException {
        JSONObject data_JSON = new JSONObject();
        data_JSON.put("OFFSET", offset);
        data_JSON.put("TYPE", "INIT-CONSUME");
        return  data_JSON.toString();
    }

    void initiateFetching() throws JSONException, IOException, TimeoutException {
        rabbitMQReceive = new RabbitMQReceive(qName, RSUAddress);
        sender.sendMessage(getInitConsumeMessage());
        rabbitMQReceive.receive(getFetchCallback());
    }

    DeliverCallback getFetchCallback(){
        return (s, delivery) -> {
            String message = new String(delivery.getBody());
            try {
                JSONObject message_json = new JSONObject(message);
                if(((String) message_json.get("TYPE")).equals("DATA")) {
                    offset = (int) message_json.get("OFFSET");
                    System.out.println("line 51... offsett ----->>");
                    System.out.println(offset);
                    writer.append(message);
                }
            } catch (JSONException e) {
                e.printStackTrace();
            }
        };
    }

}

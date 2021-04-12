import Model.MessageProps;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.TimeoutException;

public class Consumer {

    public static void startCSVConsumerFor(String topic) throws IOException, JSONException, InterruptedException {

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("group.id", "demoService");
        props.setProperty("enable.auto.commit", "true");
        props.setProperty("auto.commit.interval.ms", "1000");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topic));

        String logFileName = "E:\\GITHUB\\Simulation_KAFKA\\KAFKA_V2I_PROTOTYPE\\Services\\src\\main\\resources\\" + topic + ".csv";
        createFile(logFileName);

        FileWriter writer = new FileWriter(logFileName);
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {
            while (true) {
    //            System.out.println("consumer started on" + topic);
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
    //            long currentMillis = System.currentTimeMillis();
                for (ConsumerRecord<String, String> record : records) {

                    String message = record.value();

                    System.out.println("received a message -> " + message);

                    JSONObject messageJSON = new JSONObject(message);
    //
//                    String values = (String) messageJSON.get("DATA");

                    String carId = (String) messageJSON.get(MessageProps.CAR_ID);
                    topic = (String) messageJSON.get(MessageProps.TOPIC);

                    messageJSON.put(MessageProps.RECEIVED_BY_SERVICE_AT, System.currentTimeMillis());

                    sendMessageOnEvaluationModule(carId, topic, messageJSON.toString(), channel);

                    writer.append(messageJSON.toString());
                    writer.append("\n");

                    System.out.printf("offset = %d, key = %s", record.offset(), record.key());
                    System.out.println();
                    System.out.println("value: " + record.value());
                    Thread.sleep(5);


//                    writer.close();

                }
                }
        } catch (TimeoutException e) {
            e.printStackTrace();
        }
    }

    private static void sendMessageOnEvaluationModule(String carId, String topic, String message, Channel channel) throws IOException {

        String qName = getQNameServiceToEvaluationFor(carId,topic);

        channel.queueDeclare(qName, true, false, false, null);

        channel.basicPublish("", qName,
                MessageProperties.PERSISTENT_TEXT_PLAIN,
                message.getBytes("UTF-8"));

    }

    private static String getQNameServiceToEvaluationFor(String carId, String topic) {
        return MessageProps.SERVICE_TO_EVALUATION + "_" + carId + "_" + topic;
    }

    static void createFile(String logFileName) {
        try {
            File myObj = new File(logFileName);

            if (myObj.createNewFile()) {
//                System.out.println("File created: " + myObj.getName());
            }
        } catch (IOException e) {
//            System.out.println("An error occurred.");
            e.printStackTrace();
        }
    }
}

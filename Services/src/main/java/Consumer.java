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

public class Consumer {

    public static void startCSVConsumerFor(String topic) throws IOException, JSONException {

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("group.id", "demoService");
        props.setProperty("enable.auto.commit", "true");
        props.setProperty("auto.commit.interval.ms", "1000");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topic));

        String logFileName = "Services/src/main/resources/" + topic + ".csv";
        createFile(logFileName);

        FileWriter writer = new FileWriter(logFileName);

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            long currentMillis = System.currentTimeMillis();
            for (ConsumerRecord<String, String> record : records) {

                JSONObject messageJSON = new JSONObject(record.value());

                JSONArray values = (JSONArray) messageJSON.get("DATA");
                int values_length = values.length();

                for (int i =0 ; i< values_length; i ++) {
//                    Object val = values.get(i);                   // if we need just values..
                    writer.append(record.value());
                    values_length--;
                    if (values_length != 0) {
                        writer.append(",");
                    }
                }

                if((messageJSON.get("TYPE")).equals("HEAD"))
                    writer.append(",producerToConsumerTimeInMilliseconds\n");
                else{
                    long timestampOnProducer = (long) messageJSON.get("PRODUCER_TIMESTAMP");
                    System.out.println("difference in time: " + (currentMillis - timestampOnProducer));
                    writer.append(",");
                    writer.append(Long.toString(currentMillis - timestampOnProducer)).append("\n");
                }

                System.out.printf("offset = %d, key = %s", record.offset(), record.key());
                System.out.println();
                System.out.println("value: " + record.value());
            }
//            writer.close();                                                                                                          //todo: check posible issues with this and do we need to write this stuff ?
            //todo: check kafka monitoring tools ...
        }
    }

    static void createFile(String logFileName) {
        try {
            File myObj = new File(logFileName);

            if (myObj.createNewFile()) {
                System.out.println("File created: " + myObj.getName());
            }
        } catch (IOException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }
    }
}

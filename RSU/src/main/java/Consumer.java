import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.json.JSONException;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Properties;

public class Consumer {

    public static HashMap<String, HashMap<Long, String>> consumableDataByTopics = new HashMap<>();
    static Boolean isAccessToAccessControlPermissible = true;
    static HashMap<String, Boolean> isAccessToConsumableDataByTopicsPermissible = new HashMap<>();

    public static boolean checkAccessToConsumableDataOnTopic(String topic) throws InterruptedException {

        if(isAccessToAccessControlPermissible) {
            isAccessToAccessControlPermissible = false;
            if(isAccessToConsumableDataByTopicsPermissible.containsKey(topic)){
                isAccessToAccessControlPermissible = true;
                return isAccessToConsumableDataByTopicsPermissible.get(topic);
            }
            else{
                isAccessToAccessControlPermissible = true;
                isAccessToConsumableDataByTopicsPermissible.put(topic, true);
                return true;
            }
        }
        else{
            Thread.sleep(2);
            checkAccessToConsumableDataOnTopic(topic);
        }

        return  false;
    }

    public static void acquireAccessOnTopic(String topic){
        isAccessToConsumableDataByTopicsPermissible.replace(topic, false);
    }

    public static void looseAccessOnTopic(String topic){
        isAccessToConsumableDataByTopicsPermissible.replace(topic, true);
    }

    // Object oriented stuff from here on..
    HashMap<Long, String> newData = new HashMap<Long, String>();
    public void startConsumerFor(String topic) throws IOException, JSONException, InterruptedException {

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("group.id", "demoService");
        props.setProperty("enable.auto.commit", "true");
        props.setProperty("auto.commit.interval.ms", "1000");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topic));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, String> record : records) {
                newData.put(record.offset(), record.value());
                System.out.println(record.offset());
                System.out.println(record.value());
            }
            if(consumableDataByTopics.containsKey(topic)){
                HashMap<Long, String> dataOnTopic = consumableDataByTopics.get(topic);
                dataOnTopic.putAll(newData);
                if(checkAccessToConsumableDataOnTopic(topic)) {
                    acquireAccessOnTopic(topic);
                    consumableDataByTopics.replace(topic, dataOnTopic);
                    looseAccessOnTopic(topic);
                    newData = new HashMap<>();
                }
            }
            else{
                consumableDataByTopics.put(topic, newData);
            }
        }

    }
}

//        String logFileName = "src/main/resources/" + topic + ".csv";
//        createFile(logFileName);

//        FileWriter writer = new FileWriter(logFileName);

//        while (true) {
//            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
//            long currentMillis = System.currentTimeMillis();
//            HashMap<Long, String> newData = new HashMap<Long, String>();
//            for (ConsumerRecord<String, String> record : records) {
//                newData.put(record.offset(),record.value());
//
//                JSONObject messageJSON = new JSONObject(record.value());
//
//                JSONArray values = (JSONArray) messageJSON.get("DATA");
//                int values_length = values.length();
//
//                for (Object val : values) {
//                    writer.append(val.toString());
//                    values_length--;
//                    if (values_length != 0) {
//                        writer.append(",");
//                    }
//                }
//
//                if((messageJSON.get("TYPE")).equals("HEAD"))
//                    writer.append(",producerToConsumerTimeInMilliseconds\n");
//                else{
//                    long timestampOnProducer = (long) messageJSON.get("PRODUCER_TIMESTAMP");
//                    System.out.println("difference in time: " + (currentMillis - timestampOnProducer));
//                    writer.append(",");
//                    writer.append(Long.toString(currentMillis - timestampOnProducer)).append("\n");
//                }
//
//                System.out.printf("offset = %d, key = %s", record.offset(), record.key());
//                System.out.println();
//                System.out.println("value: " + record.value());
//            }
////            writer.close();                                                                                                          //todo: check posible issues with this and do we need to write this stuff ?
//            //todo: check kafka monitoring tools ...
//        }
//            }
//        }
//    }

//    static void createFile(String logFileName) {
//        try {
//            File myObj = new File(logFileName);
//
//            if (myObj.createNewFile()) {
//                System.out.println("File created: " + myObj.getName());
//            }
//        } catch (IOException e) {
//            System.out.println("An error occurred.");
//            e.printStackTrace();
//        }
//    }

//}

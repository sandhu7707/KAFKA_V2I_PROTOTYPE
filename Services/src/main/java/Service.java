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
import java.util.Arrays;
import java.util.Collections;
import java.util.Objects;
import java.util.Properties;

public class Service {

    //    static final String[] CSV_TOPICS = {"GPS","GPS_TIME","IMU","POSE_GROUND_TRUTH","POSE_LOCALIZED","POSE_RAW","TF","VELOCITY_RAW"};

    static final String[] CSV_TOPICS = {"GPS"};


    public static void main(String[] args){
        startProducers();
        startConsumers();
    }

    static void startConsumers() {

        Arrays.stream(CSV_TOPICS).forEach(topic -> {
            Thread t = new Thread(() -> {
                try {
                    Consumer.startCSVConsumerFor(topic);
                } catch (IOException | JSONException e) {
                    e.printStackTrace();
                }
            });
            t.start();
        });
    }

    public static void startProducers() {
        File dir = new File("src/main/java/ProducableData");
        System.out.println(Arrays.toString(dir.listFiles()));

        if (dir.isDirectory()) {

            for (final File f : Objects.requireNonNull(dir.listFiles())) {

                Thread t = new Thread(() -> {
                    try {
                        Producer producer = new Producer();
                        producer.parseOnFile(f);
                    } catch (InterruptedException | IOException | ClassNotFoundException | JSONException e) {
                        e.printStackTrace();
                    }
                });

                t.start();

            }
        }
    }

    private static void startCSVConsumerFor(String topic) throws IOException, JSONException {
        Consumer.startCSVConsumerFor(topic);
    }


}

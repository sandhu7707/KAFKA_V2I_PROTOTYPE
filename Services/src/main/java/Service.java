import org.json.JSONException;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

public class Service {

//    static final String[] CSV_TOPICS = {"DEMO_TOPIC_1", "DEMO_TOPIC_2", "DEMO_TOPIC_3", "DEMO_TOPIC_4", "DEMO_TOPIC_5"};
    static final String[] CSV_TOPICS = {"DEMO_TOPIC_0", "DEMO_TOPIC_1"};

    public static void main(String[] args){
//        startProducers();
        startConsumers();
    }

    static void startConsumers(){
        Arrays.stream(CSV_TOPICS).forEach(topic -> {
        Thread t = new Thread(() -> {
            try {
                Consumer.startCSVConsumerFor(topic);
            } catch (IOException | JSONException | InterruptedException e) {
                e.printStackTrace();
            }
        });
        t.start();
    });
}


}

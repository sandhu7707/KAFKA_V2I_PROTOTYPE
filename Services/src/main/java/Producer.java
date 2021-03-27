import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.File;
import java.io.IOException;
import java.util.Locale;
import java.util.Properties;
import java.util.Scanner;

public class Producer {

    private KafkaProducer<String, String> producer;

    void initializeProducer(){
        //starting producer
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 1);
        props.put("linger.ms", 1);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // todo: try a json or string array serializing for csvs
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<String, String>(props);
    }


    void parseOnFile(File f) throws IOException, InterruptedException, ClassNotFoundException, JSONException {

        initializeProducer();
        System.out.println(f.getName().split("\\.")[0].toUpperCase(Locale.ROOT));
        String topic = f.getName().split("\\.")[0].toUpperCase(Locale.ROOT);

        Scanner scanner = new Scanner(f);

        String[] headers = scanner.nextLine().split("\\,");
        JSONArray header_JSON = new JSONArray(headers);

        JSONObject data_JSON = new JSONObject();
        data_JSON.put("DATA", header_JSON);
        data_JSON.put("TYPE", "HEAD");
        data_JSON.put("Method", "POST");
        data_JSON.put("Request-URI", "http://www.example.com/");
        data_JSON.put("HTTP-Version", "HTTP/1.1");
        data_JSON.put("TOPIC", topic);

        String httpStr = data_JSON.toString();
//        senderSocket.send(httpStr);
        System.out.println("starting the producing.........");
        producer.send(new ProducerRecord<>(topic, (String) data_JSON.get("TYPE") , httpStr ) );

        while (scanner.hasNextLine()) {

            String[] values = lineToArray(scanner.nextLine(), headers.length);

            data_JSON.put("TYPE", "AV_DATA");
            data_JSON.put("DATA", new JSONArray(values));

            httpStr = data_JSON.toString();

//            senderSocket.send(httpStr);

            producer.send(new ProducerRecord<>(topic, (String) data_JSON.get("TYPE"), httpStr));
            //sending to Producer

            Thread.sleep(5);                                                                                                                //CSV data has max frequency of 200.
        }

//        senderSocket.send("exit");
        scanner.close();
//        senderSocket.stopServer();
    }

    String[] lineToArray(String line, int headerLength) {

        char[] charsInLine = line.toCharArray();

        int i = 0;
        int j = 0;
        boolean readCommasForCSV = true;
        String value = "";
        String[] values = new String[headerLength];

        while (i < line.length() && !(j > headerLength)) {

            char currentChar = charsInLine[i];
            if (readCommasForCSV) {

                if (currentChar == '\"' | currentChar == '\'') {
                    readCommasForCSV = false;
                } else if (currentChar == ',') {
                    values[j] = value;
                    j++;
                    value = "";
                }

            } else if (currentChar == '\"' | currentChar == '\'') {
                readCommasForCSV = true;
            }

            if (currentChar != ',') {
                value += currentChar;
            }

            if (i == line.length() - 1) {
                values[j] = value;
            }

            i++;
        }

        return values;
    }


}

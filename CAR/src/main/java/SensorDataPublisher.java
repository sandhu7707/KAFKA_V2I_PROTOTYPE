import java.io.File;
import java.io.FileNotFoundException;
import java.util.Locale;
import java.util.Scanner;
import RabbitMQ.RabbitMQSend;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class SensorDataPublisher {

    private String currentRSU;
    RabbitMQSend sender;

    SensorDataPublisher(File f, CarProps carProps, RabbitMQSend sender) throws Exception {
        this.currentRSU = carProps.nearestRSUAddress;
        this.sender = sender;
        parseOnFileAndProduceData(f, carProps);    // TODO: make an abstract way to send receive using RabbitMQ ..
    }


    void parseOnFileAndProduceData(File f, CarProps carProps) throws JSONException, FileNotFoundException, InterruptedException {


        System.out.println(f.getName().split("\\.")[0].toUpperCase(Locale.ROOT));
        String topic = f.getName().split("\\.")[0].toUpperCase(Locale.ROOT);

        Scanner scanner = new Scanner(f);

        String[] headers = scanner.nextLine().split(",");
        JSONArray header_JSON = new JSONArray(headers);

        JSONObject data_JSON = new JSONObject();
        data_JSON.put("DATA", header_JSON);
        data_JSON.put("TYPE", "HEAD");
        data_JSON.put("Origin-Id", carProps.carId);
        data_JSON.put("TOPIC", topic);

        String httpStr = data_JSON.toString();

        sender.sendMessage(httpStr);

        while (scanner.hasNextLine()) {

            if(carProps.RSUAddressGotSwitched){                                                                             //TODO: when we find an effective method for switch, will have to modify this
                while(carProps.RSUAddressGotSwitched){
                    if(carProps.nearestRSUAddress != null){                                                                 //TODO: replace null with currentRSU when addresses of RSU are different
                        currentRSU = carProps.nearestRSUAddress;
                        sender.updateReceiverAddress(currentRSU);
                        carProps.RSUAddressGotSwitched = false;
                    }
                }
            }

            String[] values = lineToArray(scanner.nextLine(), headers.length);

            data_JSON.put("TYPE", "AV_DATA");
            data_JSON.put("DATA", new JSONArray(values));

            httpStr = data_JSON.toString();
            sender.sendMessage(httpStr);


            Thread.sleep(5);                                                                                                                //CSV data has max frequency of 200.
        }

        sender.sendMessage("exit");
        scanner.close();
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

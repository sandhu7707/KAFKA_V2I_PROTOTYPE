
import RabbitMQ.RabbitMQReceive;
import RabbitMQ.RabbitMQSend;
import com.rabbitmq.client.DeliverCallback;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.File;
import java.io.IOException;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.TimeoutException;

public class Car {
    private static final String PATH_TO_SENSOR_DATA = "CAR/src/main/resources/AV_CSVs";
    CarProps carProps = new CarProps();
    String initToRSU = "INIT-TO-RSU";
    static String initToCar = "INIT-TO-CAR";

    Car() {
        Thread t = new Thread(() -> {
            try {
                startMoving();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });
        t.start();

        t = new Thread(()-> {
            RabbitMQSend initSender = new RabbitMQSend(initToRSU, carProps.nearestRSUAddress);
            try {
                initiateCommunication(carProps, initSender);
            } catch (JSONException | IOException | TimeoutException e) {
                e.printStackTrace();
            }
        });
        t.start();
    }

    /*
        Simulated movement
     */
    private void startMoving() throws InterruptedException {
        Thread.sleep(carProps.tickTime*1000);

        carProps.updateCarProps();

        System.out.println("position of " + carProps.carId + ": " + carProps.position);
        System.out.println("speed of " + carProps.carId + ": " + carProps.speed);
        System.out.println("nearestRSUAddress of " + carProps.carId + ": " + carProps.nearestRSUAddress);

        if(carProps.position < 2000) {
            startMoving();     //this will keep us moving
        }
        else{
            System.out.println(carProps.carId + " has finished it's journey !");
        }
    }

    void initiateCommunication(CarProps carProps, RabbitMQSend initSender) throws JSONException, IOException, TimeoutException {
        JSONObject data_JSON = new JSONObject();
        data_JSON.put("DATA", "");
        data_JSON.put("TYPE", "INIT");
        data_JSON.put("Origin-Id", carProps.carId);

        data_JSON.put("SENSOR_TOPICS", getAllSensorTopics());
//        data_JSON.put("CONSUMER_TOPICS", topic);
// TODO: car will know some consumer topics .. now, we have scope for on demand too with rabbitMQ..
        String msgStr = data_JSON.toString();
        initSender.sendMessage(msgStr);

        RabbitMQReceive initReceiver = new RabbitMQReceive(initToCar, carProps.nearestRSUAddress);

        startInitReceiver(initReceiver);
    }

    private JSONArray getAllSensorTopics(){
        JSONArray sensorTopics = new JSONArray();

        File dir = new File(PATH_TO_SENSOR_DATA);
        if (dir.isDirectory()) {
            for (final File f : Objects.requireNonNull(dir.listFiles())) {
                sensorTopics.put(f.getName().split("\\.")[0].toUpperCase(Locale.ROOT));
            }
        }

        return sensorTopics;
    }

    private void startInitReceiver(RabbitMQReceive initReceiver) throws IOException, TimeoutException {

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");

            JSONObject message_json = null;
            try {
                message_json = new JSONObject(message);


                switch (message_json.get("TYPE").toString()) {
                    case "INIT":
                        handleInit(message_json);
                        break;
                }

                System.out.println(message_json.get("TYPE"));

                System.out.println(" [x] Received '" + message + "'");
            } catch (JSONException e) {
                e.printStackTrace();
            }
        };

        initReceiver.receive(deliverCallback);

    }

    private void handleInit(JSONObject message_json) throws JSONException {
        Thread t = new Thread(this::produceSensorData);
        t.start();

        System.out.println(message_json.toString());
        JSONArray consumableTopics = (JSONArray) message_json.get("CONSUMABLE_TOPICS");

        for (int i = 0; i < consumableTopics.length(); i++){

            //TODO: call consumableDataReceiver for each of these and put them in a map , if consumableDataReceiver already exists, change the address for RSU and continue consuming with offset..
        }

        System.out.println(consumableTopics);
        System.out.println("line 110");

    }

    /*
        Simulated sensor data
     */
    private void produceSensorData() {
        File dir = new File(PATH_TO_SENSOR_DATA);

        if (dir.isDirectory()) {

            for (final File f : Objects.requireNonNull(dir.listFiles())) {

                Thread t = new Thread(() -> {
                    try {
                        String qName = f.getName().split("\\.")[0].toUpperCase(Locale.ROOT);
                        RabbitMQSend sender = new RabbitMQSend(qName, carProps.nearestRSUAddress);
                        new SensorDataPublisher(f, carProps, sender);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });
                t.start();

            }
        }
    }

}

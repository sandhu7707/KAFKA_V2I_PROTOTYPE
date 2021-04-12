package com.example.Evaluation;

import com.example.Evaluation.Model.MessageProps;
import com.example.Evaluation.RabbitMQ.RabbitMQSimpleReceive;
import com.rabbitmq.client.DeliverCallback;
import com.rabbitmq.client.Delivery;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.TimeoutException;

public class EvaluationIndex {
    String carId;
    JSONArray topics;
    int interval = 10; //time in seconds between automatic CSV generation

    public Boolean isAccessToAccessControlPermissible = true;
    public HashMap<String, Boolean> isAccessToEvaluationDataByTopicsPermissible = new HashMap<>();
    // TODO: set this up for multiThread access
    public HashMap<String, HashMap<String, JSONObject>> evaluationDataByTopics = new HashMap<>();

    public EvaluationIndex(String carId, JSONArray topics) throws IOException, JSONException, InterruptedException {
        this.carId = carId;
        this.topics = topics;
        startReceiverFromCar();
        startReceiverFromRSU();
        startReceiverFromServices();
        Thread t = new Thread (() -> {
            try {
                generateCSVAfterInterval(interval*1000);
            } catch (IOException | JSONException | InterruptedException e) {
                e.printStackTrace();
            }
        });

        t.start();
    }

    String[] schemaForCSVIndex = {MessageProps.MESSAGE_ID, MessageProps.TIME_CAR_TO_RSU, MessageProps.TIME_RSU_TO_SERVICE,
            MessageProps.SIMULATED_DROP_RETRANSMISSION_ATTEMPT, MessageProps.REAL_DROP_RETRANSMISSION_ATTEMPT, MessageProps.SENT_FROM_CAR_AT,
            MessageProps.RECEIVED_ON_RSU_AT, MessageProps.SENT_FROM_RSU_AT, MessageProps.RECEIVED_BY_SERVICE_AT};

    private void generateCSVAfterInterval(int interval) throws IOException, JSONException, InterruptedException {

        for(int i =0; i< topics.length(); i++ ) {

            String topic = (String) topics.get(i);

            String logFileName = "E:\\GITHUB\\Simulation_KAFKA\\KAFKA_V2I_PROTOTYPE\\Evaluation\\src\\main\\java\\com\\example\\Evaluation\\GeneratedIndex\\" + topic + ".csv";
            createFile(logFileName);
            FileWriter writer = new FileWriter(logFileName);

            for(String val: schemaForCSVIndex) {
                writer.append(val);
                writer.append(",");
            }

            writeInCSV(topic, interval, writer);

        }}

    private void writeInCSV(String topic, int interval, FileWriter writer) throws InterruptedException, IOException, JSONException {

        while(true) {

            while (true) {
                if (checkAccessToEvaluationDataOnTopic(topic)) {
                    break;
                }
            }

            HashMap<String, JSONObject> data;
            acquireAccessOnTopic(topic);
            if(evaluationDataByTopics.containsKey(topic)) {
                data = new HashMap<>(evaluationDataByTopics.get(topic));
                evaluationDataByTopics.replace(topic, new HashMap<>());
                looseAccessOnTopic(topic);

                for (JSONObject val : data.values()) {
                    System.out.println("writing in csv -> " +  val);

                    writer.append("\n");
                    for (String param : schemaForCSVIndex) {
                        writer.append((String) val.get(param));
                        writer.append(",");
                    }
                }
            }

            Thread.sleep(interval);
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

    public boolean checkAccessToEvaluationDataOnTopic(String topic) throws InterruptedException {

        if(isAccessToAccessControlPermissible) {
            isAccessToAccessControlPermissible = false;
            if(isAccessToEvaluationDataByTopicsPermissible.containsKey(topic)){
                isAccessToAccessControlPermissible = true;
                return isAccessToEvaluationDataByTopicsPermissible.get(topic);
            }
            else{
                isAccessToEvaluationDataByTopicsPermissible.put(topic, true);
                isAccessToAccessControlPermissible = true;
                return true;
            }
        }
        else{
            Thread.sleep(2);
            checkAccessToEvaluationDataOnTopic(topic);
        }

        return  false;
    }

    public void acquireAccessOnTopic(String topic){
        isAccessToEvaluationDataByTopicsPermissible.replace(topic, false);
    }

    public void looseAccessOnTopic(String topic){
        isAccessToEvaluationDataByTopicsPermissible.replace(topic, true);
    }

    void updateValueInEvaluationData(String topic, String messageId, JSONObject message_json){

        HashMap<String, JSONObject> data;
        if(!evaluationDataByTopics.containsKey(topic)){
            evaluationDataByTopics.put(topic, new HashMap<>());
        }
        data = evaluationDataByTopics.get(topic);

        while(true){
            try {
                if(checkAccessToEvaluationDataOnTopic(topic)){
                    break;
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        if(data.containsKey(messageId)){
            data.replace(messageId, message_json);
        }
        else{
            data.put(messageId, message_json);
        }

        acquireAccessOnTopic(topic);
        evaluationDataByTopics.replace(topic, data);
        looseAccessOnTopic(topic);

    }

    JSONObject getMessageFromEvaluationData(String topic, String messageId){

        HashMap<String, JSONObject> data;
        if(!evaluationDataByTopics.containsKey(topic)){
            evaluationDataByTopics.put(topic, new HashMap<>());
        }
        data = evaluationDataByTopics.get(topic);

        if(data.containsKey(topic)){
            return data.get(topic);
        }

        return null;
    }

    public void startReceiverFromCar(){

        for(int i = 0; i<topics.length(); i++){

            int finalI = i;
            Thread t = new Thread(()->{
                RabbitMQSimpleReceive receiver = null;
                try {
                    String topic = (String) topics.get(finalI);
                    receiver = new RabbitMQSimpleReceive(getCarToEvaluationDataForIndexQForTopic((String) topic));
                } catch (JSONException e) {
                    e.printStackTrace();
                }
                try {
                    receiver.receive(new DeliverCallback() {
                        @Override
                        public void handle(String s, Delivery delivery) throws IOException {
                            String message = new String(delivery.getBody(), "UTF-8");
                            try {
//                                System.out.println("received message for evaluation index -> " + message);

                                // save evaluation index as csv file when car position > 2000

                                JSONObject message_json = new JSONObject(message);
                                String topic = (String) message_json.get(MessageProps.TOPIC);
                                String messageId = (String) message_json.get(MessageProps.MESSAGE_ID);

                                Thread t = new Thread(()->{
                                    updateValueInEvaluationData(topic, messageId, message_json);
                                });

                                t.start();


                            } catch (JSONException e) {
                                e.printStackTrace();
                            }
                        }
                    });
                } catch (IOException | TimeoutException e) {
                    e.printStackTrace();
                }
            });

            t.start();
        }
    }

    public void startReceiverFromRSU(){
        for(int i = 0; i<topics.length(); i++){

            int finalI = i;
            Thread t = new Thread(()->{
                RabbitMQSimpleReceive receiver = null;
                try {
                    receiver = new RabbitMQSimpleReceive(getRSUToEvaluationDataForIndexQForTopic((String) topics.get(finalI)));
                } catch (JSONException e) {
                    e.printStackTrace();
                }
                try {
                    receiver.receive(new DeliverCallback() {
                        @Override
                        public void handle(String s, Delivery delivery) throws IOException {
                            String message = new String(delivery.getBody(), "UTF-8");
                            try {
//                                System.out.println("received message for evaluation index from RSU -> " + message);

                                // save evaluation index as csv file when car position > 2000

                                JSONObject message_json = new JSONObject(message);
                                String topic = (String) message_json.get(MessageProps.TOPIC);
                                String messageId = (String) message_json.get(MessageProps.MESSAGE_ID);


                                Thread t = new Thread(()->{
                                    try {

                                        message_json.put(MessageProps.TIME_CAR_TO_RSU,
                                                (long)message_json.get(MessageProps.RECEIVED_ON_RSU_AT) - (long)message_json.get(MessageProps.SENT_FROM_CAR_AT));


                                        updateValueInEvaluationData(topic, messageId, message_json);

                                    } catch (JSONException e) {
                                        e.printStackTrace();
                                    }

                                });

                                t.start();



                            } catch (JSONException e) {
                                e.printStackTrace();
                            }
                        }
                    });
                } catch (IOException | TimeoutException e) {
                    e.printStackTrace();
                }
            });

            t.start();
        }
    }

    public void startReceiverFromServices(){
        for(int i = 0; i<topics.length(); i++){

            int finalI = i;
            Thread t = new Thread(()->{
                RabbitMQSimpleReceive receiver = null;
                try {
                    receiver = new RabbitMQSimpleReceive(getQNameServiceToEvaluationFor((String) topics.get(finalI)));
                } catch (JSONException e) {
                    e.printStackTrace();
                }
                try {
                    receiver.receive(new DeliverCallback() {
                        @Override
                        public void handle(String s, Delivery delivery) throws IOException {
                            String message = new String(delivery.getBody(), "UTF-8");
                            try {

                                // save evaluation index as csv file when car position > 2000

                                JSONObject message_json = new JSONObject(message);
                                String topic = (String) message_json.get(MessageProps.TOPIC);
                                String messageId = (String) message_json.get(MessageProps.MESSAGE_ID);


                                Thread t = new Thread(()->{
                                    try {
                                        JSONObject existingMessage = getMessageFromEvaluationData(topic, messageId);
                                        JSONObject final_message_json;

                                        if(existingMessage != null){
                                            final_message_json = existingMessage;
                                        }
                                        else{
                                            final_message_json = message_json;
                                        }

                                        final_message_json.put(MessageProps.TIME_CAR_TO_RSU,
                                                (long)message_json.get(MessageProps.RECEIVED_ON_RSU_AT) - (long)message_json.get(MessageProps.SENT_FROM_CAR_AT));

                                        final_message_json.put(MessageProps.TIME_RSU_TO_SERVICE,
                                                (long)message_json.get(MessageProps.RECEIVED_BY_SERVICE_AT) - (long)message_json.get(MessageProps.SENT_FROM_RSU_AT));


                                        updateValueInEvaluationData(topic, messageId, final_message_json);


//                                        writeInCSV(final_message_json);

                                    } catch (JSONException e) {
                                        e.printStackTrace();
                                    }

                                });

                                t.start();


                            } catch (JSONException e) {
                                e.printStackTrace();
                            }
                        }
                    });
                } catch (IOException | TimeoutException e) {
                    e.printStackTrace();
                }
            });

            t.start();
        }
    }

//    private void writeInCSV(JSONObject message_json) {
//        System.out.println("will write in csv -> " + message_json.toString());
//    }

    String getCarToEvaluationDataForIndexQForTopic(String topic){
        return MessageProps.CAR_TO_EVALUATION_INDEXING_PREFIX + "_" + carId + "_" + topic;
    }

    String getRSUToEvaluationDataForIndexQForTopic(String topic){
        return MessageProps.RSU_TO_EVALUATION_INDEXING_PREFIX + "_" + carId + "_" + topic;
    }

    private String getQNameServiceToEvaluationFor( String topic) {
        return MessageProps.SERVICE_TO_EVALUATION + "_" + carId + "_" + topic;
    }

}

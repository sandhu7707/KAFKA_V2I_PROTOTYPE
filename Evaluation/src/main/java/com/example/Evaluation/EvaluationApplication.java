package com.example.Evaluation;

import org.json.JSONException;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

//@SpringBootApplication
public class EvaluationApplication {

	public static String EVALUATION_APPLICATION_ADDRESS = "localhost";
	public static String mode = "EVALUATION";
	public static int messagePriority = 1;
	public static int numberOfTopics = 2;
	public static int messageSize = 1;
	public static boolean stopSimulation = false;

	public static void main(String[] args) {
//		SpringApplication.run(EvaluationApplication.class, args);

//		Thread t = new Thread(()-> {
//			MessageReceiver messageReceiver = new MessageReceiver();
//		});

		Thread t = new Thread(()-> {
			CarInstance carInstance = new CarInstance();
			try {
				carInstance.StartCarInstance( 0.005, mode, messagePriority, numberOfTopics, messageSize);

			} catch (JSONException | TimeoutException | IOException | InterruptedException e) {
				e.printStackTrace();
			}
		});

		t.start();

	}

}

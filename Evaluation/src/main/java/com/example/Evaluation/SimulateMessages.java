package com.example.Evaluation;

public class SimulateMessages {

    public String simulatedMessageData;

    SimulateMessages(int msgSizeInKbs){
        simulatedMessageData = createMessageOfSize(msgSizeInKbs);
    }

    private String createMessageOfSize(int msgSizeInKbs) {

        // leaving 100 chars for overhead..

        int stringSize = (msgSizeInKbs*1024/2) - 100;
        StringBuilder sb = new StringBuilder(stringSize);
        for (int i=0; i<stringSize; i++) {
            sb.append('*');
        }
        return sb.toString();
    }



}

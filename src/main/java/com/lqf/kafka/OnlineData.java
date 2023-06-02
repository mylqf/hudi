package com.lqf.kafka;

import com.google.gson.JsonObject;

import java.util.Random;

public class OnlineData {

    public static String[] generateOnlineData(){

        String[] returns= new String[2];

        JsonObject jsonObject = new JsonObject();
        Random random = new Random();
        jsonObject.addProperty("session_id","FF0000001B34");
        jsonObject.addProperty("from_cluster","clife-iot-platform-middleware-proxy");


        JsonObject general_message = new JsonObject();
        jsonObject.add("general_message",general_message);

        String[] s = {"offline","online"};
        int i=0;
        i = random.nextInt(2);
        general_message.addProperty("command",s[i]);

//        String[] tags={"14_13_50520_10787",
//                "14_13_50520_10788",
//                "14_13_50520_10789",
//                "7_14_50464_2157",
//                "7_17_51091_7567",
//                "27_3_50994_11480",
//                "80_1_50994_5398",
//                "82_30_50994_5832"};

        String[] tags={"TagA"};


        String tag=tags[random.nextInt(tags.length)];

        returns[0]=tag;


        general_message.addProperty("macAddress","1");

        JsonObject data = new JsonObject();

        general_message.add("data",data);
        Random randDid = new Random();
        String[] did = {"1","2","3","4","5","6","7","8","9","10"};
        int j=0;
        j= randDid.nextInt(10);
        data.addProperty("deviceId",did[j]);

        data.addProperty("record_time",System.currentTimeMillis());

//        System.out.println(jsonObject);


        returns[1]=jsonObject.toString();

        return returns;

    }

    public static void main(String[] args) {

        System.out.println(generateOnlineData()[1]);
    }
}

package com.enzo.bigdata.spark.stream;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

/**
 * @Classname Sparksql01_Env
 * @Description TODO
 * @Date 2024/6/24 16:28
 * @Created by Enzo
 */
public class SparkStream02_Socket {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf();
        conf.setMaster("local[*]");
        conf.setAppName("Spark Stream");

        JavaStreamingContext jsc = new JavaStreamingContext(conf, new Duration(3 * 1000));

        JavaReceiverInputDStream<String> dstream = jsc.socketTextStream("localhost", 9999);
        dstream.print();


        jsc.start();
        jsc.awaitTermination();

    }
}

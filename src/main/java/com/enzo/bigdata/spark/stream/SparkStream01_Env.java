package com.enzo.bigdata.spark.stream;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

/**
 * @Classname Sparksql01_Env
 * @Description TODO
 * @Date 2024/6/24 16:28
 * @Created by Enzo
 */
public class SparkStream01_Env {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf();
        conf.setMaster("local[*]");
        conf.setAppName("Spark Stream");

        // TODO 对Spark Core的环境和数据模型进行了封装
        // 第二个参数，批量的数据采集周期 时间
        JavaStreamingContext jsc = new JavaStreamingContext(conf, new Duration(3 * 1000));

        // TODO 启动采集器
        jsc.start();

        // 等待阻塞 等待采集器的中止
        jsc.awaitTermination();

    }
}

package com.enzo.bigdata.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.function.Consumer;

/**
 * @Classname SparkCore01_Env
 * @Description TODO
 * @Date 2024/6/18 14:19
 * @Created by Enzo
 */
public class SparkCore03_Disk {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf();
        conf.setMaster("local[2]");
        conf.setAppName("Spark Core Test");
        final JavaSparkContext jsc = new JavaSparkContext(conf);

        final JavaRDD<String> rdd = jsc.textFile("F:\\atguigu\\16-spark\\code\\SparkCore1.8\\src\\main\\java\\data\\a.txt");
        rdd.collect().forEach(new Consumer<String>() {
            @Override
            public void accept(String s) {
                System.out.println(s);
            }
        });

        jsc.close();
    }
}

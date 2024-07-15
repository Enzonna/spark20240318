package com.enzo.bigdata.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import java.io.Serializable;
import java.util.Arrays;

/**
 * @Classname SparkCore01_Function
 * @Description TODO
 * @Date 2024/6/19 14:20
 * @Created by Enzo
 */
public class SparkCore05_part {
    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf();
        conf.setMaster("local[*]");
        conf.setAppName("Spark Core Test");
        final JavaSparkContext jsc = new JavaSparkContext(conf);

        User user = new User();

        final Broadcast<User> broadcast = jsc.broadcast(user);

        final JavaRDD<Integer> rdd = jsc.parallelize(
                Arrays.asList(1, 2, 4, 3), 2
        );

        rdd.foreach(
                num -> {
                    System.out.println("++++++++");
                    System.out.println(broadcast.getValue().age + num);
                }
        );

        jsc.close();
    }
}

class User implements Serializable {
    public int age = 30;

}
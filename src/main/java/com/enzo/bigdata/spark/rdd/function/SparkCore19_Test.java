package com.enzo.bigdata.spark.rdd.function;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * @Classname SparkCore01_Function
 * @Description TODO
 * @Date 2024/6/19 14:20
 * @Created by Enzo
 */
public class SparkCore19_Test {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local[*]");
        conf.setAppName("Test");
        final JavaSparkContext jsc = new JavaSparkContext(conf);

        jsc.textFile("data/a.txt")
                .flatMap(line -> Arrays.asList(line.split(" ")).iterator())
                .mapToPair(name -> new Tuple2<>(name, 1))
                .reduceByKey(Integer::sum)
                .collect()
                .forEach(System.out::println);

        System.out.println("++++++++++++++++++++++++++++++++++++++++++++++++++++++++");

        jsc.textFile("data/a.txt")
                .flatMap(line -> Arrays.asList(line.split(" ")).iterator())
                .mapToPair(name -> new Tuple2<>(name, 1))
                .groupByKey()
                .collect()
                .forEach(System.out::println);


        jsc.close();
    }
}


package com.enzo.bigdata.spark.rdd.function;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

/**
 * @Classname SparkCore01_Function
 * @Description TODO
 * @Date 2024/6/19 14:20
 * @Created by Enzo
 */
public class SparkCore04_Function_groupBy_2 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local[*]");
        conf.setAppName("Spark Core Test");
        final JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaRDD<String> rdd = jsc.parallelize(
                Arrays.asList("Hadoop", "hive", "Spark", "sqoop"), 2
        );

        rdd.groupBy(
                name -> name.substring(0, 1).toLowerCase()
                //name -> name.charAt(0)
                //name -> name.substring(0, 1)
        ).collect().forEach(System.out::println);

        jsc.close();
    }
}


package com.enzo.bigdata.spark.rdd.function;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/**
 * @Classname SparkCore01_Function
 * @Description TODO
 * @Date 2024/6/19 14:20
 * @Created by Enzo
 */
public class SparkCore04_Function_flatMap_1 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local[*]");
        conf.setAppName("Spark Core Test");
        final JavaSparkContext jsc = new JavaSparkContext(conf);

        // 将一个整体拆分成个体使用的操作，称之为扁平化(flat + map)操作
        JavaRDD<List<Integer>> rdd = jsc.parallelize(
                Arrays.asList(
                        Arrays.asList(1, 2, 3),
                        Arrays.asList(4, 5, 6)
                )
        );

        JavaRDD<Integer> flatRDD = rdd.flatMap(
                List::iterator
        );

        flatRDD.collect().forEach(System.out::println);

        jsc.close();
    }
}


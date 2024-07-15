package com.enzo.bigdata.spark.rdd.function;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * @Classname SparkCore01_Function
 * @Description TODO
 * @Date 2024/6/19 14:20
 * @Created by Enzo
 */
public class SparkCore10_Function_groupByKey {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local[*]");
        conf.setAppName("Spark Core Test");
        final JavaSparkContext jsc = new JavaSparkContext(conf);

        final JavaPairRDD<String, Integer> rdd = jsc.parallelizePairs(
                Arrays.asList(
                        new Tuple2<>("a", 20),
                        new Tuple2<>("b", 2000),
                        new Tuple2<>("a", 291),
                        new Tuple2<>("b", 988)
                ), 2
        );

        // 将key作为分组的标记，将V分在一个组中
        // 求和方便
        rdd.groupByKey().collect().forEach(System.out::println);

        jsc.close();
    }
}


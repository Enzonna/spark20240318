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
public class SparkCore10_Function_coalesce {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local[*]");
        conf.setAppName("Spark Core Test");
        final JavaSparkContext jsc = new JavaSparkContext(conf);

        final JavaPairRDD<String, Integer> rdd = jsc.parallelizePairs(
                Arrays.asList(
                        new Tuple2<>("a", 20),
                        new Tuple2<>("b", 2000),
                        new Tuple2<>("c", 291),
                        new Tuple2<>("d", 988)
                ), 4
        );

        // coalesce() 合并分区，缩减分区，直接改变分区的方法
        // 该方法默认没shuffle操作，数据不会被打乱
        // repartition() 扩大分区
        // 底层就是coalesce()
        JavaPairRDD<String, Integer> coalesceRDD = rdd.coalesce(2);
        coalesceRDD.saveAsTextFile("output");

        jsc.close();
    }
}


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
public class SparkCore08_Function_KV_mapValues {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local[*]");
        conf.setAppName("Spark Core Test");
        final JavaSparkContext jsc = new JavaSparkContext(conf);

        final JavaPairRDD<String, Integer> rdd = jsc.parallelizePairs(
                Arrays.asList(
                        // 元组是final 不可变 tuple2._2 元组的数据只是为了封装，无法进行修改
                        new Tuple2<>("enzo", 20),
                        new Tuple2<>("enzo2", 2000),
                        new Tuple2<>("enzo3", 291),
                        new Tuple2<>("enzo4", 988)
                ), 2
        );

        // key不变，对value做转换
        rdd.mapValues(
                amount -> amount * 10
        ).collect().forEach(System.out::println);

        jsc.close();
    }
}


package com.enzo.bigdata.spark.rdd.function;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
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
public class SparkCore08_Function_KV {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local[*]");
        conf.setAppName("Spark Core Test");
        final JavaSparkContext jsc = new JavaSparkContext(conf);

        // 之前的都是单值类型
        // KV类型 二元组 数据的KEY和VALUE独立来考虑的类型 key是key，value是value，需要采用特殊方法来对接数据源

//        jsc.parallelize(
//                Arrays.asList(
//                        new Tuple2<>(1001, "enzo"),
//                        new Tuple2<>(1002, "enzo2"),
//                        new Tuple2<>(1003, "enzo3"),
//                        new Tuple2<>(1004, "enzo4")
//                ), 2
//        );
        JavaPairRDD<Integer, String> rdd = jsc.parallelizePairs(
                Arrays.asList(
                        new Tuple2<>(1001, "enzo"),
                        new Tuple2<>(1002, "enzo2"),
                        new Tuple2<>(1003, "enzo3"),
                        new Tuple2<>(1004, "enzo4")
                ), 2
        );

        //rdd.mapValues()

        rdd.collect().forEach(System.out::println);

        jsc.close();
    }
}


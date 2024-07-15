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
public class SparkCore04_Function_groupBy_3 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local[*]");
        conf.setAppName("Spark Core Test");
        final JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaRDD<Integer> rdd = jsc.parallelize(
                Arrays.asList(1, 2, 3, 4), 2
        );

        // Spark要求一个组的数据必须在同一个分区中，不允许跨分区
        // Spark的分组操作，会将一个分区的数据打乱，和其他分区的数据从新组合在一起
        // 这个操作称之为 打乱重组 ，称为 shuffle , 洗牌
        // Spark要求shuffle操作中，前一个RDD功能不执行完，后续的RDD功能不能继续
        // Spark中RDD不允许存数据，Shuffle操作又必须保证前一个RDD完全处理完，才能执行后续RDD，那么我们的数据就需要临时保存
        // 关键 : Spark的shuffle操作是将数据存储在 磁盘 中，一定要落盘，不在RDD,决定性能的关键，shuffle的优化是整体优化的关键

        // 第二个参数是分组后分区的数量
        rdd.groupBy(
                num -> num % 2, 2
        ).collect().forEach(System.out::println);

        // Spark中的shuffle操作必须要落盘(写入磁盘)

        jsc.close();
    }
}


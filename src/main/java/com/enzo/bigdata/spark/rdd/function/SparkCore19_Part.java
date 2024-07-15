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
public class SparkCore19_Part {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local[*]");
        conf.setAppName("Test");
        final JavaSparkContext jsc = new JavaSparkContext(conf);

        // TODO 数据分区器 Partitioner
        JavaRDD<Integer> rdd = jsc.parallelize(
                Arrays.asList(1, 2, 3, 4), 2
        );

        JavaPairRDD<Integer, Integer> kvRDD = rdd.mapToPair(
                num -> new Tuple2<>(num, 1)
        );

        // shuffle会改变默认的分区规则
        //      Spark会使用默认的分区器HashPartitioner，来改变默认规则
        JavaPairRDD<Integer, Iterable<Integer>> groupRDD = kvRDD.groupByKey();

        groupRDD.saveAsTextFile("output");

        jsc.close();
    }
}


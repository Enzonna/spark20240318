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
public class SparkCore10_Function_groupByKey_1 {
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

        // 分组
        //JavaPairRDD<String, Iterable<Integer>> rdd2 = rdd.groupByKey();

        // 聚合
        // Spark为了提高分组聚合的效率，开发效率，运行效率，对方法的调用进行了优化
        // reduceByKey 将分组聚合合二为一
        // 相同key的value放置在一起，将分组后的value数据进行reduce聚合
        // 使用reduceByKey 开发效率变快了

        //reduceByKey优先选择！！！预聚合
        rdd.reduceByKey(
                Integer::sum
        ).collect().forEach(System.out::println);

        jsc.close();
    }
}


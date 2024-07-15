package com.enzo.bigdata.spark.rdd.function;

import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * @Classname SparkCore01_Function
 * @Description TODO
 * @Date 2024/6/19 14:20
 * @Created by Enzo
 */
public class SparkCore10_Function_collect {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local[*]");
        conf.setAppName("Spark Core Test");
        final JavaSparkContext jsc = new JavaSparkContext(conf);

        final JavaRDD<Integer> rdd = jsc.parallelize(
                Arrays.asList(1, 2, 3, 4), 4
        );

        final JavaRDD<Integer> mapRDD = rdd.map(
                num -> {
                    System.out.println("num = " + num);
                    return num;
                }
        );

        // Spark RDD 的方法主要功能不是执行逻辑，而是组合逻辑
        //      collect方法的执行会触发Spark逻辑的执行
        //      RDD的方法主要分为2大类
        //          1. 组合功能： A -> B(A) -> C(B(A))
        //          2. 执行功能： collect
        //      区分方式：
        //          组合功能的方法都会返回RDD对象，等同于将旧的RDD转化成新的RDD，用于组合功能 -> 转换算子
        //          执行功能的方法都会返回具体的执行结果，和RDD无关 -> 执行算子
        //          直接看返回值就行

        final JavaRDD<Integer> sortRDD = rdd.sortBy(num -> num, true, 2);
        final List<Integer> result = mapRDD.collect();

        jsc.close();
    }
}


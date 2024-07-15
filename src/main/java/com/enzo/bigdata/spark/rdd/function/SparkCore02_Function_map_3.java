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
public class SparkCore02_Function_map_3 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local[*]");
        conf.setAppName("Spark Core Test");

        final JavaSparkContext jsc = new JavaSparkContext(conf);

        final JavaRDD<Integer> rdd = jsc.parallelize(Arrays.asList(1, 2, 3, 4), 2);

        // RDD 执行流程
        // RDD 不留数据，RDD只是管道，让你执行一段逻辑，并不会保存数据！
        final JavaRDD<Integer> mapRDD = rdd.map(
                num -> {
                    System.out.println("first_num = " + num);
                    return num * 2;
                }
        );

        final JavaRDD<Integer> mapRDD1 = mapRDD.map(
                num -> {
                    System.out.println("second_num = " + num);
                    return num * 2;
                }
        );

        mapRDD1.collect();

        jsc.close();
    }
}

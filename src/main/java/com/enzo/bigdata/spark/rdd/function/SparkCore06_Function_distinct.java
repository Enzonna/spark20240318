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
public class SparkCore06_Function_distinct {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local[*]");
        conf.setAppName("Spark Core Test");
        final JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaRDD<Integer> rdd = jsc.parallelize(
                Arrays.asList(1, 1, 3, 4), 4
        );

        // distinct : 去重
        // distinct 也会产生数据倾斜
        // 第二个参数，去重后分区的数量
        JavaRDD<Integer> distinctRDD = rdd.distinct();
        distinctRDD.collect().forEach(System.out::println);

        jsc.close();
    }
}


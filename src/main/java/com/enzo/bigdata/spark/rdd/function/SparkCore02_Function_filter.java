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
public class SparkCore02_Function_filter {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local[*]");
        conf.setAppName("Spark Core Test");

        final JavaSparkContext jsc = new JavaSparkContext(conf);
        final JavaRDD<Integer> rdd = jsc.parallelize(Arrays.asList(1, 2, 3, 4), 2);

        // TODO : filter 过滤
        //        filter()可以按照对数据指定的规则进行筛选过滤
        //        filter()会根据规则的返回值来判断这个数据到底要不要，true就要，false就不要，里面方法只能返回boolean值
        // 但是可能会导致数据倾斜 -> 分区数量扩大，有可能解决，但不一定一定解决
        JavaRDD<Integer> filterRDD = rdd.filter(
                num -> num % 2 == 0
        );

        filterRDD.collect().forEach(System.out::println);

        jsc.close();
    }
}

package com.enzo.bigdata.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class SparkCore02_RDD_Instance_Memory_Partition_Data {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf();
        conf.setMaster("local[*]");
        conf.setAppName("Spark Core Test");
        final JavaSparkContext jsc = new JavaSparkContext(conf);

        List<Integer> nums = Arrays.asList(1, 2, 3, 4, 5, 6);

        final JavaRDD<Integer> rdd = jsc.parallelize(nums, 3);

        // Spark分区策略：平均分
        // 【1，2，3】
        // 【4，5，6】
        // Spark对接内存数据源时，分区数据分配策略使用了简单算法，具体的不想听
        // hadoop切片规则是按字节计算的
        // Spark没有，用的就是hadoop的规则，用听吗？
        rdd.saveAsTextFile("output");
        jsc.stop();
    }
}

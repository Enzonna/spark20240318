package com.enzo.bigdata.spark.rdd.function;

import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * @Classname SparkCore01_Function
 * @Description TODO
 * @Date 2024/6/19 14:20
 * @Created by Enzo
 */
public class SparkCore15_Function_save {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local[*]");
        conf.setAppName("Spark Core Test");
        final JavaSparkContext jsc = new JavaSparkContext(conf);

        final JavaRDD<Integer> rdd = jsc.parallelize(
                Arrays.asList(1, 2, 4, 3), 2
        );

//        rdd.saveAsTextFile("output");
//        rdd.saveAsObjectFile("output1");
//        rdd.foreach(
//                word -> System.out.println("word = " + word)
//        );

        // 迭代器 运行快 但是慎用 会将一个分区的数据统一进行传递并进行处理，性能比较高
        // 慎用 在内存，会溢出，占用很多的资源，有时候也不是速度越快越好，比如安全性
        rdd.foreachPartition(
                iter -> {
                    while (iter.hasNext()) {
                        System.out.println(iter.next());
                    }
                }
        );

        jsc.close();
    }
}


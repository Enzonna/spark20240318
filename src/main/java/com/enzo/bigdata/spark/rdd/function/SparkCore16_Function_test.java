package com.enzo.bigdata.spark.rdd.function;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;
import java.util.Arrays;

/**
 * @Classname SparkCore01_Function
 * @Description TODO
 * @Date 2024/6/19 14:20
 * @Created by Enzo
 */
public class SparkCore16_Function_test {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local[*]");
        conf.setAppName("Spark Core Test");
        final JavaSparkContext jsc = new JavaSparkContext(conf);

        final JavaRDD<Integer> rdd = jsc.parallelize(
                Arrays.asList(1, 2, 3, 4), 2
        );

        Student s = new Student();

        //拉取后在循环
//        rdd.collect().forEach(
//                num -> {
//                    System.out.println(s.age + num);
//                }
//        );

        //分布式循环
        rdd.foreach(
                num -> {
                    System.out.println(s.age + num);
                }
        );

        jsc.close();
    }
}

// 为什么要序列化？
// foreach
// RDD的功能组合实在Driver端执行的，但是执行在Executor执行的
// 分清哪些代码在Driver端，哪些代码在Executor端
// 算子中逻辑是在Executor端运行的，算子外的代码逻辑是在Driver端运行的
// 如果Executor端用到了Driver端的对象，那么就得序列化
// 序列就是数组
class Student implements Serializable {
    public int age = 30;

}

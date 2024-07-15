package com.enzo.bigdata.spark.est;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class SparkCore14_Function_collect {
    public static void main(String[] args) throws Exception {


        SparkConf conf = new SparkConf();
        conf.setMaster("local[*]");
        conf.setAppName("Spark Core RDD Function");
        final JavaSparkContext jsc = new JavaSparkContext(conf);

        final JavaRDD<Integer> rdd = jsc.parallelize(
                Arrays.asList(1, 2, 3, 4), 4
        );

//        final JavaRDD<Integer> mapRDD = rdd.map(
//                num -> {
//                    System.out.println("num = " + num);
//                    return num;
//                }
//        );

        // TODO Spark RDD的方法主要功能不是执行逻辑，是组合逻辑
        //      collect方法的执行会触发Spark逻辑的执行
        //      RDD的方法主要分为2大类
        //           1. 组合功能 : A -> B(A) -> C(B(A))
        //           2. 执行功能 : collect
        //       区分方式：看方法的返回值类型
        //           组合功能的方法都会返回RDD对象，等同于将旧的RDD转转成新的RDD，用于组合功能
        //               一般称之为：【转换算子】
        //           执行功能的方法都会返回具体的执行结果，和RDD无关，称之为：【行动算子】
        //       网上（书籍）：判断作业是否执行
        //       所谓的算子，其实就是RDD的方法， Scala集合中的方法称之为方法
        final JavaRDD<Integer> sortRDD = rdd.sortBy(num -> num, true, 2);
        final List<Integer> result = sortRDD.collect();

        result.forEach(System.out::println);
        // http://localhost:4040
        Thread.sleep(10000000000L);


        jsc.stop();
    }
}

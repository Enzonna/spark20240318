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
public class SparkCore02_Function_map_1 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        // local | local[] | local[*]
        conf.setMaster("local[*]");
        conf.setAppName("Spark Core Test");

        final JavaSparkContext jsc = new JavaSparkContext(conf);

        // 分区 numSlices
        // 默认情况下，RDD的分区数量和前一个RDD的分区数量相等
        // 默认情况下，数据处理后的分区和数据处理前的分区相同
        // 分区间无序，分区内有序
        final JavaRDD<Integer> rdd = jsc.parallelize(Arrays.asList(1, 2, 3, 4), 2);
        // map()可以做任意的转换,将一个数据转换为另一个数据(1:1)
        // int 可以变成 int,String,User,List,null....
        final JavaRDD<Integer> mapRDD = rdd.map(
                num -> {
                    System.out.println("num = " + num);
                    return num * 2;
                }
        );

        //mapRDD.saveAsTextFile("output");//.collect();//.forEach(System.out::println);
        mapRDD.collect();

        jsc.close();
    }
}

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
public class SparkCore04_Function_flatMap {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local[*]");
        conf.setAppName("Spark Core Test");

        final JavaSparkContext jsc = new JavaSparkContext(conf);
        // 将文件的技术名称分别打印到控制台
        // map能做的只能1:1，但是该业务要1:N

        // flatMap 将数据分解成多个数据返回，但是语法要求返回值必须是迭代器
        JavaRDD<String> rdd = jsc.textFile("data/a.txt");

        //返回迭代器
        JavaRDD<String> flatMapRDD = rdd.flatMap(
                line -> Arrays.asList(line.split(" ")).iterator()
        );

        flatMapRDD.collect().forEach(System.out::println);

        jsc.close();
    }
}

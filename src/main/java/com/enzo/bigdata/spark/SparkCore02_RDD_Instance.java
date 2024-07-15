package com.enzo.bigdata.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

/**
 * @Classname SparkCore01_Env
 * @Description TODO
 * @Date 2024/6/18 14:19
 * @Created by Enzo
 */
public class SparkCore02_RDD_Instance {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf();
        conf.setMaster("local[2]");
        conf.setAppName("Spark Core Test");
        final JavaSparkContext jsc = new JavaSparkContext(conf);

        // TODO 获取RDD分布式计算模型
        //      RDD模型需要对接数据源，不同的数据源的对接方式不一样。
        //      1. Memory
        //      2. Disk File
        //
        // TODO 将内存数据作为数据源，获取RDD模型对象
        List<String> names = Arrays.asList(
                "enzo", "enzo2", "enzo3"
        );

        final JavaRDD<String> rdd = jsc.parallelize(names);

        rdd.collect().forEach(new Consumer<String>() {
            @Override
            public void accept(String s) {
                System.out.println(s);
            }
        });


        jsc.close();
    }
}

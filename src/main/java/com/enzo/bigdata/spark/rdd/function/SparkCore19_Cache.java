package com.enzo.bigdata.spark.rdd.function;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * @Classname SparkCore01_Function
 * @Description TODO
 * @Date 2024/6/19 14:20
 * @Created by Enzo
 */
public class SparkCore19_Cache {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local[*]");
        conf.setAppName("Test");
        final JavaSparkContext jsc = new JavaSparkContext(conf);


        JavaPairRDD<String, Integer> kvRDD = jsc.textFile("data/a.txt")
                .flatMap(line -> Arrays.asList(line.split(" ")).iterator())
                .mapToPair(name -> new Tuple2<>(name, 1));

        // cache() 放进缓存(内存)，提高性能，不在磁盘交互了，因为RDD不存数据
        // 将中间计算结果保存起来给其他的RDD使用
        // 在RDD重复使用的时候使用
        // cache底层就是persist，封装后的
        // cache 会在血缘关系中增加一条依赖

        // persist() 持久化
        kvRDD.cache();
        // 存到本地
        jsc.setCheckpointDir("cp");

        // 联合使用
        kvRDD.cache();
        kvRDD.checkpoint();     //跨应用，会创建一个新的血缘关系

        jsc.close();
    }
}


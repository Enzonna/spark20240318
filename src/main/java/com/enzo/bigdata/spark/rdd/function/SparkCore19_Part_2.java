package com.enzo.bigdata.spark.rdd.function;

import org.apache.spark.Partitioner;
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
public class SparkCore19_Part_2 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local[*]");
        conf.setAppName("Test");
        final JavaSparkContext jsc = new JavaSparkContext(conf);

        // 分区器主要针对于KV类型
        JavaPairRDD<String, String> rdd = jsc.parallelizePairs(
                Arrays.asList(
                        new Tuple2<>("enzo", "bb"),
                        new Tuple2<>("Mafyv", "aa"),
                        new Tuple2<>("enzo", "cc"),
                        new Tuple2<>("Mafyv", "dd")
                ), 2
        );

        JavaPairRDD<String, Iterable<String>> groupRDD = rdd.groupByKey(new MyPartitioner());

        groupRDD.saveAsTextFile("output2");


        jsc.close();
    }
}

// 步骤；
// 1. 继承抽象类
// 2. 重写
class MyPartitioner extends Partitioner {
    // 定义分区数量
    @Override
    public int numPartitions() {
        return 3;
    }

    // 根据key的值返回数据所在的分区编号
    @Override
    public int getPartition(Object key) {
        if ("Mafyv".equals(key)){
            return (int)'m';
        }else if ("enzo".equals(key)){
            return (int)'s';
        }else {
            return 0;
        }
    }
}
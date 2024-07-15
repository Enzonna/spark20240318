package com.enzo.bigdata.spark.rdd.function;

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
public class SparkCore15_Function_action {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local[*]");
        conf.setAppName("Spark Core Test");
        final JavaSparkContext jsc = new JavaSparkContext(conf);

        final JavaRDD<Integer> rdd = jsc.parallelize(
                Arrays.asList(1, 2, 4, 3), 4
        );

        long count = rdd.count();
        Integer first = rdd.first();
        // take,拿top3
        List<Integer> take = rdd.take(3);
        // 排序后的top3
        List<Integer> integers = rdd.takeOrdered(3);

        // 将单值数据类型转化为KV键值类型
        JavaPairRDD<Integer, Integer> pairRDD = rdd.mapToPair(
                num -> new Tuple2<>(num, num)
        );

        Map<Integer, Long> map = pairRDD.countByKey();
        System.out.println(map);

        Map<Tuple2<Integer, Integer>, Long> value = pairRDD.countByValue();
        System.out.println(value);



//        System.out.println(count);
//        System.out.println(first);
//        System.out.println(take);
//        System.out.println(integers);
        jsc.close();
    }
}


package com.enzo.bigdata.spark.rdd.function;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/**
 * @Classname SparkCore01_Function
 * @Description TODO
 * @Date 2024/6/19 14:20
 * @Created by Enzo
 */
public class SparkCore04_Function_groupBy {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local[*]");
        conf.setAppName("Spark Core Test");
        final JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaRDD<Integer> rdd = jsc.parallelize(
                Arrays.asList(1, 2, 3, 4), 2
        );

        // TODO groupBy() 按照...分组
        // Spark要求一个组的数据必须在同一个分区中，不允许跨分区


//        // groupBy() 会将每一条数据增加分组的标记，相同标记的数据就会分在一个组中,分组标记就是分组后的组名
//        rdd.groupBy(
//                num -> {
//                    if (num % 2 == 0) {
//                        return true;
//                    } else {
//                        return false;
//                    }
//                }
//        ).collect().forEach(System.out::println);

        rdd.groupBy(num -> num % 2).collect().forEach(System.out::println);

        jsc.close();
    }
}


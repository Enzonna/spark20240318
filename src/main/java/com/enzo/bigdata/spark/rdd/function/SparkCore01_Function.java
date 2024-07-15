package com.enzo.bigdata.spark.rdd.function;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.Arrays;


/**
 * @Classname SparkCore01_Function
 * @Description TODO
 * @Date 2024/6/19 14:20
 * @Created by Enzo
 */
public class SparkCore01_Function {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local[*]");
        conf.setAppName("Spark Core Test");
        final JavaSparkContext jsc = new JavaSparkContext(conf);

        //只作读的功能
        final JavaRDD<Integer> rdd = jsc.parallelize(
                Arrays.asList(1, 2, 3, 4)
        );

        //TODO 逻辑 ： num * 2
        //只能转,做封装，两个rdd组合在一起，装饰者设计模式
        //蠢b模式，聪明模式见02
        final JavaRDD<Integer> mapRDD =  rdd.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer in) throws Exception {
                return in * 2;
            }
        });

        mapRDD.collect().forEach(System.out::println);

        jsc.close();
    }
}

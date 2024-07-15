package com.enzo.bigdata.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * @Classname SparkCore01_Env
 * @Description TODO
 * @Date 2024/6/18 14:19
 * @Created by Enzo
 */
public class SparkCore01_Env {
    public static void main(String[] args) {

        // TODO : 获取开发环境
        //      RDD
        SparkConf conf = new SparkConf();
        conf.setMaster("local[2]");
        conf.setAppName("Spark Core Test");
        final JavaSparkContext jsc = new JavaSparkContext(conf);


        // TODO : 代码逻辑


        // TODO : 释放资源
        jsc.close();
    }
}

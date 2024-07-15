package com.enzo.bigdata.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkCore03_RDD_Instance_Disk_Partition_Data {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf();
        //conf.setMaster("local");
        conf.setAppName("Spark Core Test");
        final JavaSparkContext jsc = new JavaSparkContext(conf);

        final JavaRDD<String> rdd = jsc.textFile("data/a.txt",2);

        rdd.saveAsTextFile("output");

        jsc.stop();
    }
}

package com.enzo.bigdata.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkCore03_RDD_Instance_Disk_Partition {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf();
        conf.setMaster("local[*]");
        conf.setAppName("Spark Core Test");
        final JavaSparkContext jsc = new JavaSparkContext(conf);

        // TODO textFile()主要用于对接磁盘文件数据源，获取RDD数据模型
        //      方法有两个参数：1. 磁盘文件路径
        //                    2. 最小分区数量(参数可以省略，但会采用默认值，取math.min(总核数，2))

        // TODO 文件数据的分区数量取决于Hadoop的文件切片
        //      取决于字节数，取决于hadoop的分区规则
        final JavaRDD<String> rdd = jsc.textFile("data/a.txt",4);

        rdd.saveAsTextFile("output");

        jsc.stop();
    }
}

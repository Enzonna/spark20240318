package com.enzo.bigdata.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

public class SparkCore02_RDD_Instance_Memory_Partition {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf();
        conf.setMaster("local[*]");
        conf.setAppName("Spark Core Test");
        //conf.set("spark.default.parallelism", "5");
        final JavaSparkContext jsc = new JavaSparkContext(conf);

        List<String> names = Arrays.asList(
                "zhangsan", "lisi", "wangwu"
        );

        // TODO parallelize方法表示并行处理数据集,可以传递多个参数
        //      1. 第一个参数表示对接数据源
        //      2. 第二个参数表示切片数量（在Hadoop中，数据规模的切分就称之为切片，而Spark就称之为分区）
        //             此参数可以省略，会采用默认值
        //                    scheduler.conf.getInt("spark.default.parallelism", totalCores)
       // final JavaRDD<String> rdd = jsc.parallelize(names, 3);
        final JavaRDD<String> rdd = jsc.parallelize(names,3);

        //saveAsTextFile方法可以将RDD的数据分区保存成文本文件
        // TODO 当前分区数量为2
        rdd.saveAsTextFile("output");


        jsc.stop();
    }
}

package com.enzo.bigdata.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkCore03_RDD_Instance_Disk_1 {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf();
        conf.setMaster("local[2]");
        conf.setAppName("Spark Core Test");
        final JavaSparkContext jsc = new JavaSparkContext(conf);

        final JavaRDD<String> rdd = jsc.textFile("data/a.txt");

        // Java语言不适合大数据的开发: JDK1.8 => 函数式编程 => lambda表达式
        //      (IN) -> {OUT}
        // Scala语言适合大数据的开发
        // 在Java语言中，如果接口使用@FunctionalInterface注解，那么意味着可以使用函数式进行简化操作
//        rdd.collect().forEach(new Consumer<String>() {
//            @Override
//            public void accept(String s) {
//                System.out.println(s);
//            }
//        });
        rdd.collect().forEach(
                (String s) -> {
                    System.out.println(s);
                }
        );


        jsc.stop();
    }
}

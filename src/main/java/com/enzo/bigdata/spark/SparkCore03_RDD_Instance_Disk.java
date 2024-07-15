package com.enzo.bigdata.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public class SparkCore03_RDD_Instance_Disk {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf();
        conf.setMaster("local[2]");
        conf.setAppName("Spark Core Test");
        final JavaSparkContext jsc = new JavaSparkContext(conf);

        // TODO 对接磁盘文件数据源，获取RDD数据模型对象
        //      textFile方法可以传递一个参数，参数表示文件路径：
        //      1.绝对路径 : 不可改变的路径
        //           协议://路径
        //           web  :  http://IP:PORT/path
        //           hdfs :  hdfs://IP:PORT/path
        //           local : file:///root/xxxx
        //      2.相对路径 : 可以改变的路径（取决于基准路径）
        //           IDEA : 以【项目】的根路径为基准
        //final JavaRDD<String> rdd = jsc.textFile("D:\\idea\\classes\\bigdata-bj-classes240318\\spark\\data\\test.txt");
        final JavaRDD<String> rdd = jsc.textFile("src/main/java/data/a.txt");

        // Java语言不适合大数据的开发: JDK1.8 => 函数式编程 => lambda表达式
        // Scala语言适合大数据的开发
        rdd.collect().forEach(new Consumer<String>() {
            @Override
            public void accept(String s) {
                System.out.println(s);
            }
        });


        jsc.stop();
    }
}

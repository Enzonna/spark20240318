package com.enzo.bigdata.spark.sql;

import com.enzo.bigdata.spark.est.MyAvgAgeUDAF;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;

public class SparkSql08_Source_CSV {
    public static void main(String[] args) {


        SparkConf conf = new SparkConf();
        conf.setMaster("local[*]");
        conf.setAppName("Spark SQL");

        final SparkSession sparkSQL = SparkSession.builder().config(conf).getOrCreate();

        // csv : 数据表示逗号分隔开
        final Dataset<Row> csvDS = sparkSQL.read()
                .option("header", true)  // 读取列名
                .option("sep", "_")     //tsv,用什么分割
                .csv("data/data.csv");

        // TODO 写入数据
        //      默认情况下输出的文件路径是不允许存在的，否则发生错误，但是可以更改保存模式，存在一起
        // UUID : 随机字符串
        csvDS.write()
                .mode(SaveMode.Append)      //存储方式
                .option("header", true)  // 读取列名
                .option("sep", "_")     //tsv,用什么分割
                .csv("output");


        sparkSQL.stop();
    }
}



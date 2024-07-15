package com.enzo.bigdata.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.util.parsing.json.package$;

/**
 * @Classname Sparksql01_Env
 * @Description TODO
 * @Date 2024/6/24 16:28
 * @Created by Enzo
 */
public class SparkSql01_Env {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local[*]");
        conf.setAppName("Spark SQL");
        // Spark在结构化数据的处理场合，使用了新的模块SparkSQL，会将RDD数据模型进行封装，同时也对环境进行封装
        // Session 会话 类似于JDBC中的Connection
        // SparkSQL的环境构造方式比较复杂，不能直接new，采用设计模式：构建器模式
        // 当构建对象的步骤和过程比较复杂的场合，构建器模型将步骤进行封装，变化的内容可以通过外部实现
        SparkSession sparkSQL = SparkSession
                .builder()
                .appName("SparkSQL")
                .master("local[*]")
                .getOrCreate();

        // 使用新的环境对象构建新的数据模型Dataset
        // SparkSQL是基于Spark Core开发的功能模块，底层文件的读取操作还用的Hadoop
        Dataset<Row> jsonDS = sparkSQL.read().json("data/user.json");
        jsonDS.show();

        // 数据源中的数据映射成一张表
        jsonDS.createOrReplaceTempView("user");


        Dataset<Row> result = sparkSQL.sql("select avg(age) from user");
        result.show();


        sparkSQL.stop();
    }
}

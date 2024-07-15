package com.enzo.bigdata.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Aggregator;

import java.io.Serializable;


/**
 * @Classname Sparksql01_Env
 * @Description TODO
 * @Date 2024/6/24 16:28
 * @Created by Enzo
 */
public class SparkSql06_Function_UDAF {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local[*]");
        conf.setAppName("Spark SQL");

        final SparkSession sparkSQL = SparkSession.builder().config(conf).getOrCreate();

        final Dataset<Row> jsonDS = sparkSQL.read().json("data/user.json");
        jsonDS.createOrReplaceTempView("user");

        // functions.udaf() 参数： 1. 聚合函数类
        //                        2. 输入数据编码对象
        sparkSQL.udf().register("avgAge", functions.udaf(
                new MyAvgAgeUDAF(), Encoders.LONG()
        ));

        sparkSQL.sql("select avgAge(age) from user").show();

        sparkSQL.stop();
    }
}


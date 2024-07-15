package com.enzo.bigdata.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * @Classname Sparksql01_Env
 * @Description TODO
 * @Date 2024/6/24 16:28
 * @Created by Enzo
 */
public class SparkSql01_Dataset_DSL {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local[*]");
        conf.setAppName("Spark SQL");
        SparkSession sparkSQL = SparkSession.builder().config(conf).getOrCreate();

        Dataset<Row> jsonDS = sparkSQL.read().json("data/user.json");

        // TODO 采用DSL
        jsonDS.select("age","name").show();




        sparkSQL.stop();
    }
}


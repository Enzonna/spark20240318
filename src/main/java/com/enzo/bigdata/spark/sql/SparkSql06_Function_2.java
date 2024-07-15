package com.enzo.bigdata.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

/**
 * @Classname Sparksql01_Env
 * @Description TODO
 * @Date 2024/6/24 16:28
 * @Created by Enzo
 */
public class SparkSql06_Function_2 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local[*]");
        conf.setAppName("Spark SQL");

        SparkSession sparkSQL = SparkSession.builder().config(conf).getOrCreate();

        Dataset<Row> jsonDS = sparkSQL.read().json("data/user.json");
        jsonDS.createOrReplaceTempView("user");

        sparkSQL.udf().register("prefixName", new UDF1<String, String>() {
            @Override
            public String call(String name) throws Exception {
                return "name : " + name;
            }
        }, DataTypes.StringType);
        sparkSQL.sql("select prefixName(name) from user").show();


        sparkSQL.stop();
    }
}


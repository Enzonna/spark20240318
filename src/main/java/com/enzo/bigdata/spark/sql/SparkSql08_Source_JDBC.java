package com.enzo.bigdata.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class SparkSql08_Source_JDBC {
    public static void main(String[] args) {


        SparkConf conf = new SparkConf();
        conf.setMaster("local[*]");
        conf.setAppName("Spark SQL");

        final SparkSession sparkSQL = SparkSession.builder().config(conf).getOrCreate();

        //sparkSQL.read().jdbc("??");

        sparkSQL.stop();
    }
}



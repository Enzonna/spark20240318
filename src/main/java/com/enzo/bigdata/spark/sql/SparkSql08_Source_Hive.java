package com.enzo.bigdata.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkSql08_Source_Hive {
    public static void main(String[] args) {
        //默认情况下，IDEA执行Hive访问时，会采用系统的登录用户作为Hive的访问用户
        System.setProperty("HADOOP_USER_NAME","enzo");

        SparkConf conf = new SparkConf();
        conf.setMaster("local[*]");
        conf.setAppName("Spark SQL");

        final SparkSession sparkSQL = SparkSession.builder()
                .enableHiveSupport()
                .config(conf).getOrCreate();

        sparkSQL.sql("show tables").show();

        sparkSQL.sql("create table user_info(name String,age bigint)");
        sparkSQL.sql("insert into table user_info values('zhangsan',10)");
        sparkSQL.sql("select * from user_info").show();

        sparkSQL.stop();
    }
}



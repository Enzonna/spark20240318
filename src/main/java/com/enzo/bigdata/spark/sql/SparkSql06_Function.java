package com.enzo.bigdata.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

import javax.naming.Name;

/**
 * @Classname Sparksql01_Env
 * @Description TODO
 * @Date 2024/6/24 16:28
 * @Created by Enzo
 */
public class SparkSql06_Function {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local[*]");
        conf.setAppName("Spark SQL");

        SparkSession sparkSQL = SparkSession.builder().config(conf).getOrCreate();

        Dataset<Row> jsonDS = sparkSQL.read().json("data/user.json");
        jsonDS.createOrReplaceTempView("user");

        // TODO 将每一行数据的名称加一个前缀 name：
        // TODO 自定义函数功能
        // 1. 向Spark注册函数功能 传递函数名称，需要和SQL中使用的名称保持一致
        //                      根据函数的参数决定使用哪一个类型的参数,泛型看输入输出类型
        // 2. 在SQL文中使用函数功能
        //                      使用时，需要和注册时保持一致
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


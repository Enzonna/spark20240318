package com.enzo.bigdata.spark.est;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.expressions.Aggregator;
import org.apache.spark.sql.types.DataTypes;
import scala.Serializable;

public class SparkSql08_Function_UDAF {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local[*]");
        conf.setAppName("Spark SQL");

        final SparkSession sparkSQL = SparkSession.builder().config(conf).getOrCreate();

        final Dataset<Row> jsonDS = sparkSQL.read().json("data/user.json");
        jsonDS.createOrReplaceTempView("user");

        // TODO Spark当前版本的UDAF需要调用特殊的方法来使用 : functions.udaf
        //     udaf函数需要传递2个参数：
        //        第一个参数表示聚合函数类
        //        第二个参数表示输入数据编码对象
        sparkSQL.udf().register("avgAge", functions.udaf(
                new MyAvgAgeUDAF(), Encoders.LONG()
        ));

        sparkSQL.sql("select avgAge(age) from user").show();


        sparkSQL.stop();
    }
}



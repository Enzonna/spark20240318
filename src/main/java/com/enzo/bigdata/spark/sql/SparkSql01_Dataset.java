package com.enzo.bigdata.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.*;

/**
 * @Classname Sparksql01_Env
 * @Description TODO
 * @Date 2024/6/24 16:28
 * @Created by Enzo
 */
public class SparkSql01_Dataset {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local[*]");
        conf.setAppName("Spark SQL");

        SparkSession sparkSQL = SparkSession.builder().config(conf).getOrCreate();

        Dataset<Row> jsonDS = sparkSQL.read().json("data/user.json");

        // 将Row类型转换成自定义对象
        Dataset<User> userDS = jsonDS.as(Encoders.bean(User.class));
        //JavaRDD<User> userJavaRDD = userDS.javaRDD();
        //userDS.show();

        // TODO 简化开发 将数据模型转换为临时视图 Hive -> Tool
        // 视图和表的区别： 视图就是查询结果集，SQL操作只能执行查询操作
        jsonDS.createOrReplaceTempView("user");
        sparkSQL.sql("select * from user").show();


        sparkSQL.stop();
    }
}

class User {
    private String name;
    // 文件中的数据为整型数据，自定义对象要用Long
    private Long age;

    public User() {
    }

    public User(String name, Long age) {
        this.name = name;
        this.age = age;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Long getAge() {
        return age;
    }

    public void setAge(Long age) {
        this.age = age;
    }

    @Override
    public String toString() {
        return "User{" +
                "name='" + name + '\'' +
                ", age=" + age +
                '}';
    }
}

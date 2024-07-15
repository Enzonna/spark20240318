package com.enzo.bigdata.spark.rdd.function;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;
import java.util.Arrays;

/**
 * @Classname SparkCore01_Function
 * @Description TODO
 * @Date 2024/6/19 14:20
 * @Created by Enzo
 */
public class SparkCore07_Function_sortBy {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local[*]");
        conf.setAppName("Spark Core Test");
        final JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaRDD<User> rdd = jsc.parallelize(
                Arrays.asList(
                        new User(20, 3000),
                        new User(30, 2000),
                        new User(10, 3000),
                        new User(70, 5000),
                        new User(70, 4000),
                        new User(70, 6000)
                ), 2
        );

        // sortBy() 排序
        // 参数1 排序的规则 Spark会给每条数据增加一个排序的标记，根据Spark给的标记(类型)进行排序
        // 参数2 排序的方式 true升序； 参数3 排序后的分区数量
        rdd.sortBy(
                //按字符串排序
                //num -> num + "",
                user -> user,
                true,
                2
        ).collect().forEach(System.out::println);
        jsc.close();
    }
}

class User implements Serializable, Comparable<User> {
    private int age;
    private int amount;

    public User() {
    }

    public User(int age, int amount) {
        this.age = age;
        this.amount = amount;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public int getAmount() {
        return amount;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }

    @Override
    public String toString() {
        return "User{" +
                "age=" + age +
                ", amount=" + amount +
                '}';
    }

    @Override
    public int compareTo(User other) {
        if (this.age > other.age) {
            return 1;
        } else if (this.age < other.age) {
            return -1;
        } else {
            return this.amount - other.amount;
        }
    }
}


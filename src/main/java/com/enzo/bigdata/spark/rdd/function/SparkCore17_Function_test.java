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
public class SparkCore17_Function_test {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local[*]");
        conf.setAppName("Spark Core Test");
        final JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaRDD<String> rdd = jsc.parallelize(
                Arrays.asList(
                        "Spark", "Sqoop", "Flink", "Hadoop", "Hive"
                )
        );

        Search search = new Search("H");
        search.match(rdd);

        //filterRDD.collect().forEach(System.out::println);

        jsc.close();
    }
}

class Search implements Serializable {
    private final String query;

    public Search(String query) {
        this.query = query;
    }

    public void match(JavaRDD<String> rdd) {
        rdd.filter(
                name -> name.startsWith(query)
        ).collect().forEach(System.out::println);
    }
}


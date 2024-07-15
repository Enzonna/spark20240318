package com.enzo.bigdata.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

public class SparkSql08_Req_Mock {
    public static void main(String[] args) {
        //默认情况下，IDEA执行Hive访问时，会采用系统的登录用户作为Hive的访问用户
        System.setProperty("HADOOP_USER_NAME", "enzo");

        SparkConf conf = new SparkConf();
        conf.setMaster("local[*]");
        conf.setAppName("Spark SQL");

        final SparkSession sparkSQL = SparkSession.builder()
                .enableHiveSupport()
                .config(conf).getOrCreate();

        sparkSQL.sql("use sparksql240318;");

        sparkSQL.sql("CREATE TABLE `user_visit_action`(\n" +
                "  `date` string,\n" +
                "  `user_id` bigint,\n" +
                "  `session_id` string,\n" +
                "  `page_id` bigint,\n" +
                "  `action_time` string,\n" +
                "  `search_keyword` string,\n" +
                "  `click_category_id` bigint,\n" +
                "  `click_product_id` bigint, --点击商品id，没有商品用-1表示。\n" +
                "  `order_category_ids` string,\n" +
                "  `order_product_ids` string,\n" +
                "  `pay_category_ids` string,\n" +
                "  `pay_product_ids` string,\n" +
                "  `city_id` bigint --城市id\n" +
                ")\n" +
                "row format delimited fields terminated by '\\t';");

        sparkSQL.sql("load data local inpath 'data/user_visit_action.txt' into table user_visit_action;");

        sparkSQL.sql("CREATE TABLE `city_info`(\n" +
                "  `city_id` bigint, --城市id\n" +
                "  `city_name` string, --城市名称\n" +
                "  `area` string --区域名称\n" +
                ")\n" +
                "row format delimited fields terminated by '\\t';");

        sparkSQL.sql("load data local inpath 'data/city_info.txt' into table city_info;");
        sparkSQL.sql("CREATE TABLE `product_info`(\n" +
                "  `product_id` bigint, -- 商品id\n" +
                "  `product_name` string, --商品名称\n" +
                "  `extend_info` string\n" +
                ")\n" +
                "row format delimited fields terminated by '\\t';");
        sparkSQL.sql("load data local inpath 'data/product_info.txt' into table product_info;");

        sparkSQL.sql("select * from product_info limit 5").show();

        sparkSQL.stop();
    }
}



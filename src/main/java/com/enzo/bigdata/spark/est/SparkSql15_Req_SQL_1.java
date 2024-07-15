package com.enzo.bigdata.spark.est;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

public class SparkSql15_Req_SQL_1 {
    public static void main(String[] args) {
        // 18801
        // 默认情况下，IDEA执行Hive访问时，会采用系统的登录用户作为Hive的访问用户
        System.setProperty("HADOOP_USER_NAME","enzo");

        SparkConf conf = new SparkConf();
        conf.setMaster("local[*]");
        conf.setAppName("Spark SQL");

        final SparkSession sparkSQL = SparkSession.builder()
                .enableHiveSupport()
                .config(conf).getOrCreate();

        sparkSQL.sql("use sparksql240318;");

        // TODO 读取用户行为数据：只保留点击数据
        sparkSQL.sql("\t\t\tselect\n" +
                "\t\t\t\t*\n" +
                "\t\t\tfrom user_visit_action\n" +
                "\t\t\twhere click_product_id != -1").createOrReplaceTempView("a");

        // TODo 查询城市信息
        sparkSQL.sql("\t\t\tselect\n" +
                "\t\t\t\tcity_id,\n" +
                "\t\t\t\tcity_name,\n" +
                "\t\t\t\tarea\n" +
                "\t\t\tfrom city_info").createOrReplaceTempView("c");

        // TODO 查询商品信息
        sparkSQL.sql("\t\t\tselect\n" +
                "\t\t\t\tproduct_id,\n" +
                "\t\t\t\tproduct_name\n" +
                "\t\t\tfrom product_info").createOrReplaceTempView("p");

        // TODO 关联3张表的数据
        sparkSQL.sql("\t\tselect\n" +
                "\t\t\tarea,\n" +
                "\t\t\tclick_product_id,\n" +
                "\t\t\tproduct_name\n" +
                "\t\tfrom a\n" +
                "\t\tjoin c on a.city_id = c.city_id\n" +
                "\t\tjoin p on a.click_product_id = p.product_id").createOrReplaceTempView("t");

        // TODO 将关联后的数据进行分组聚合
        sparkSQL.sql("\t\tselect\n" +
                "\t\t\tarea,\n" +
                "\t\t\tproduct_name,\n" +
                "\t\t\tcount(*) clickCount\n" +
                "\t\tfrom t\n" +
                "\t\tgroup by area, product_id, product_name").createOrReplaceTempView("t1");

        // TODO 将聚合后的结果进行开窗，实现组内排序
        sparkSQL.sql("\tselect\n" +
                "\t\t*,\n" +
                "\t\trank() over ( partition by area order by clickCount desc ) rk\n" +
                "\tfrom t1").createOrReplaceTempView("t2");

        // TODo 将排序后的结果取前3名
        sparkSQL.sql("select\n" +
                "\tarea,\n" +
                "\tproduct_name,\n" +
                "\tclickCount\n" +
                "from t2 where rk <= 3").show();


        sparkSQL.stop();
    }
}



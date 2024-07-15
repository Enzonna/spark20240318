package com.enzo.bigdata.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

public class SparkSql08_Req_SQL_new {
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

        // TODO 读取用户行为数据。只保留点击数据
        sparkSQL.sql("select \n" +
                "\t\t\t* \n" +
                "\t\tfrom user_visit_action\n" +
                "\t\twhere click_product_id != -1").createOrReplaceTempView("a");

        // TODO 查询城市信息
        sparkSQL.sql("select \n" +
                "\t\t\t\tcity_id,\n" +
                "\t\t\t\tcity_name,\n" +
                "\t\t\t\tarea\n" +
                "\t\tfrom city_info").createOrReplaceTempView("c");

        // TODO 查询商品信息
        sparkSQL.sql("select \n" +
                "\t\t\t\tproduct_id,\n" +
                "\t\t\t\tproduct_name\n" +
                "\t\tfrom product_info").createOrReplaceTempView("p");

        // TODO 关联三张表的数据
        sparkSQL.sql("select \n" +
                "\t\tarea,\n" +
                "\t\tproduct_name,\n" +
                "\t\tclick_product_id\n" +
                "\tfrom a\n" +
                "\tjoin c on a.city_id = c.city_id\n" +
                "\tjoin p on a.click_product_id = p.product_id\n").createOrReplaceTempView("t");

        // TODO 将关联后的数据进行分组聚合
        sparkSQL.sql("select \n" +
                "\t\tarea,\n" +
                "\t\tproduct_name,\n" +
                "\t\tcount(*) clickCount\n" +
                "\tfrom t\n" +
                "\tgroup by area, product_id,product_name").createOrReplaceTempView("t1");

        // TODO 将聚合后的结果进行开窗，实现组内排序
        sparkSQL.sql("select \n" +
                "\t\t*,\n" +
                "\t\trank() over(partition by area order by clickCount desc) rk\n" +
                "\tfrom t1").createOrReplaceTempView("t2");

        // TODO 将排序后的结果取前三名
        sparkSQL.sql("select\n" +
                "\tarea,\n" +
                "\tproduct_name,\n" +
                "\tclickCount\n" +
                "from t2\n" +
                "where rk <=3;\n").show();

        sparkSQL.stop();


    }
}



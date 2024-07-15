package com.enzo.bigdata.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

public class SparkSql08_Req_SQL {
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

//        sparkSQL.sql("select \n" +
//                "\tarea,\n" +
//                "\t\tproduct_name,\n" +
//                "\t\tcount(*) clickCount\n" +
//                "\tfrom (\n" +
//                "\tselect \n" +
//                "\t\t* \n" +
//                "\tfrom user_visit_action\n" +
//                "\twhere click_product_id != -1\n" +
//                ") act\n" +
//                "join (\n" +
//                "\tselect \n" +
//                "\t\t\tcity_id,\n" +
//                "\t\t\tcity_name,\n" +
//                "\t\t\tarea\n" +
//                "\tfrom city_info\n" +
//                ") c on act.city_id = c.city_id\n" +
//                "join (\n" +
//                "\tselect \n" +
//                "\t\t\tproduct_id,\n" +
//                "\t\t\tproduct_name\n" +
//                "\tfrom product_info\n" +
//                ") p on act.click_product_id = p.product_id\n" +
//                "group by area, product_id,product_name;").show(10);

        // TODO 组内排序
        // 不要使用group by(数据不能减少)，不能使用order by(全局排序)
        // 开窗函数
        sparkSQL.sql("select\n" +
                "\tarea,\n" +
                "\tproduct_name,\n" +
                "\tclickCount\n" +
                "from(\n" +
                "\tselect \n" +
                "\t\t*,\n" +
                "\t\trank() over(partition by area order by clickCount desc) rk\n" +
                "\tfrom (\n" +
                "\tselect \n" +
                "\t\tarea,\n" +
                "\t\t\tproduct_name,\n" +
                "\t\t\tcount(*) clickCount\n" +
                "\t\tfrom (\n" +
                "\t\tselect \n" +
                "\t\t\t* \n" +
                "\t\tfrom user_visit_action\n" +
                "\t\twhere click_product_id != -1\n" +
                "\t) act\n" +
                "\tjoin (\n" +
                "\t\tselect \n" +
                "\t\t\t\tcity_id,\n" +
                "\t\t\t\tcity_name,\n" +
                "\t\t\t\tarea\n" +
                "\t\tfrom city_info\n" +
                "\t) c on act.city_id = c.city_id\n" +
                "\tjoin (\n" +
                "\t\tselect \n" +
                "\t\t\t\tproduct_id,\n" +
                "\t\t\t\tproduct_name\n" +
                "\t\tfrom product_info\n" +
                "\t) p on act.click_product_id = p.product_id\n" +
                "\tgroup by area, product_id,product_name\n" +
                "\t) t\n" +
                ") t1\n" +
                "where rk <=3;").show();

        sparkSQL.stop();
    }
}



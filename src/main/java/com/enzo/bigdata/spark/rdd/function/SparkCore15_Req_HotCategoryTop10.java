package com.enzo.bigdata.spark.rdd.function;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * @Classname SparkCore01_Function
 * @Description TODO
 * @Date 2024/6/19 14:20
 * @Created by Enzo
 */
public class SparkCore15_Req_HotCategoryTop10 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local[*]");
        conf.setAppName("HotCategoryTop10");
        final JavaSparkContext jsc = new JavaSparkContext(conf);

        // TODO 对接数据源(本地磁盘对象) 一行一行字符串
        JavaRDD<String> dataRDD = jsc.textFile("data/user_visit_action.txt");

        // TODO 多什么，删什么，无效数据过滤
        final JavaRDD<String> filterRDD = dataRDD.filter(
                data -> {
                    final String[] s = data.split("_");
                    return "null".equals(s[5]);
                }
        );

        // TODO 缺什么，补什么
//        final JavaPairRDD<String, CategoryCountBean> pairRDD = filterRDD.mapToPair(
//                data -> {
//                    final String[] datas = data.split("_");
//                    if ("-1".equals(datas[6])) {
//                        // TODO 点击行为
//                        CategoryCountBean bean = new CategoryCountBean(datas[6], 1L, 0L, 0L);
//
//                        return new Tuple2<>(datas[6], bean);
//                    } else if (!"null".equals(datas[8])) {
//                        // TODO 下单行为
//                        String[] ids = datas[8].split(",");
//
//                        for (String id : ids) {
//                            CategoryCountBean bean = new CategoryCountBean(datas[8], 0L, 1L, 0L);
//                        }
//
//                        return new Tuple2<>(datas[8], bean);
//                    } else {
//                        // TODO 支付行为
//                        CategoryCountBean bean = new CategoryCountBean(datas[10], 0L, 0L, 1L);
//
//                        return new Tuple2<>(datas[10], bean);
//                    }
//                }
//        );
        JavaRDD<CategoryCountBean> beanRDD = filterRDD.flatMap(
                data -> {
                    final String[] datas = data.split("_");
                    if (!"-1".equals(datas[6])) {
                        // TODO 点击行为
                        CategoryCountBean bean = new CategoryCountBean(datas[6], 1L, 0L, 0L);
                        return Collections.singletonList(bean).iterator();
                    } else if (!"null".equals(datas[8])) {
                        // TODO 下单行为
                        final String[] ids = datas[8].split(",");
                        List<CategoryCountBean> beanList = new ArrayList<>();
                        for (String id : ids) {
                            CategoryCountBean bean = new CategoryCountBean(id, 0L, 1L, 0L);
                            beanList.add(bean);
                        }
                        return beanList.iterator();
                    } else {
                        // TODO 支付行为
                        final String[] ids = datas[10].split(",");
                        List<CategoryCountBean> beanList = new ArrayList<>();
                        for (String id : ids) {
                            CategoryCountBean bean = new CategoryCountBean(id, 0L, 0L, 1L);
                            beanList.add(bean);
                        }
                        return beanList.iterator();
                    }
                }

        );

        JavaPairRDD<String, CategoryCountBean> pairRDD = beanRDD.mapToPair(
                bean -> new Tuple2<>(bean.getCid(), bean)
        );

        // 相同品类进行点击，下单，支付统计
        //      reduceByKey: (id, [bean, bean, bean]) => (id, bean)
        //            (品类(1001)，点击(1)，下单，支付)
        //            (品类(1001)，点击，下单(1)，支付)
        // -------------------------------------------
        //            (品类(1001)，点击(1)，下单(1)，支付)
        //            (品类(1001)，点击，下单，支付(1))
        //   -----------------------------------------
        //            (品类(1001)，点击(1)，下单(1)，支付(1))
        final JavaPairRDD<String, CategoryCountBean> reduceRDD = pairRDD.reduceByKey(
                (bean1, bean2) -> {
                    return new CategoryCountBean(
                            bean1.getCid(),
                            bean1.getClickCount() + bean2.getClickCount(),
                            bean2.getOrderCount() + bean2.getOrderCount(),
                            bean1.getPayCount() + bean2.getPayCount()
                    );
                }
        );


        // TODO 声明自定义Bean对象，为了自定义排序
        final JavaRDD<CategoryCountBean> mapRDD = reduceRDD.map(
                kv -> kv._2
        );

        final JavaRDD<CategoryCountBean> sortRDD = mapRDD.sortBy(
                bean -> bean,
                true,
                2
        );

        // TODO 排序后取前十名

        sortRDD.take(10).forEach(System.out::println);


        jsc.close();
    }
};

// TODO 基础的Bean对象
class CategoryCountBean implements Serializable, Comparable<CategoryCountBean> {
    private String cid;
    private Long clickCount;
    private Long orderCount;
    private Long payCount;

    public CategoryCountBean() {
    }

    public CategoryCountBean(String cid, Long clickCount, Long orderCount, Long payCount) {
        this.cid = cid;
        this.clickCount = clickCount;
        this.orderCount = orderCount;
        this.payCount = payCount;
    }

    public String getCid() {
        return cid;
    }

    public void setCid(String cid) {
        this.cid = cid;
    }

    public Long getClickCount() {
        return clickCount;
    }

    public void setClickCount(Long clickCount) {
        this.clickCount = clickCount;
    }

    public Long getOrderCount() {
        return orderCount;
    }

    public void setOrderCount(Long orderCount) {
        this.orderCount = orderCount;
    }

    public Long getPayCount() {
        return payCount;
    }

    public void setPayCount(Long payCount) {
        this.payCount = payCount;
    }

    @Override
    public String toString() {
        return "CategoryCountBean{" +
                "cid='" + cid + '\'' +
                ", clickCount=" + clickCount +
                ", orderCount=" + orderCount +
                ", payCount=" + payCount +
                '}';
    }

    @Override
    public int compareTo(CategoryCountBean o) {
        if (this.clickCount > o.clickCount) {
            return -1;
        } else if (this.clickCount < o.clickCount) {
            return 1;
        } else {
            if (this.orderCount > o.orderCount) {
                return -1;
            } else if (this.orderCount < o.orderCount) {
                return 1;
            } else {
                return (int) (o.payCount - this.payCount);
            }
        }
    }
}
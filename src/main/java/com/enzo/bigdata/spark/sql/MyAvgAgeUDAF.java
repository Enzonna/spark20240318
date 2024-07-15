package com.enzo.bigdata.spark.sql;

import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.expressions.Aggregator;

/**
 * @Classname MyAvgAgeUDAF
 * @Description TODO
 * @Date 2024/6/25 10:41
 * @Created by Enzo
 */

// 自定义聚合函数类
// 0. 创建公共类
// 1. 继承抽象类
// 2. 设置泛型 : IN输入类型,BUFF缓冲区,OUT输出类型
// 3. 重写抽象方法（6） 4 + 2(固定写法)
class MyAvgAgeUDAF extends Aggregator<Long, AvgAgeBuffer, Long> {
    @Override
    // Zero 初始化 缓冲区的初始化
    public AvgAgeBuffer zero() {
        return new AvgAgeBuffer(0L, 0);
    }

    @Override
    // reduce 聚合 将输入的数据和缓冲区的数据进行聚合
    public AvgAgeBuffer reduce(AvgAgeBuffer buff, Long in) {
        buff.setCnt(buff.getCnt() + 1);
        buff.setTotal(buff.getTotal() + in);
        return buff;
    }

    @Override
    // merge 合并
    public AvgAgeBuffer merge(AvgAgeBuffer b1, AvgAgeBuffer b2) {
        b1.setTotal(b1.getTotal() + b2.getTotal());
        b1.setCnt(b1.getCnt() + b2.getCnt());
        return b1;
    }

    @Override
    // finish 完成计算
    public Long finish(AvgAgeBuffer buff) {
        return buff.getTotal() / buff.getCnt();
    }

    @Override
    public Encoder<AvgAgeBuffer> bufferEncoder() {
        return Encoders.bean(AvgAgeBuffer.class);
    }

    @Override
    public Encoder<Long> outputEncoder() {
        return Encoders.LONG();
    }
}

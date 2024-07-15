package com.enzo.bigdata.spark.est;

import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.expressions.Aggregator;

// TODO 自定义聚合函数类
//      0. 创建公共类
//      1. 继承org.apache.spark.sql.expressions.Aggregator抽象类
//      2. 设置泛型
//          IN : 约束了函数的输入数据类型
//          BUFF : 约束了函数的缓冲区数据类型
//          OUT : 约束了函数的输出数据类型
//      3. 重写抽象方法 （6 = 4 + 2）
public class MyAvgAgeUDAF extends Aggregator<Long, AvgAgeBuffer, Long> {
    @Override
    // Zero表示初始化
    // ZeroValue : initValue
    // TODO 缓冲区的初始化
    public AvgAgeBuffer zero() {
        return new AvgAgeBuffer(0L, 0);
    }

    @Override
    // reduce表示聚合
    // TODO 将输入的数据和缓冲区的数据进行聚合操作
    public AvgAgeBuffer reduce(AvgAgeBuffer buff, Long in) {

        buff.setCnt( buff.getCnt() + 1 );
        buff.setTotal( buff.getTotal() + in );

        return buff;
    }

    @Override
    // TODO merge表示合并 mergeSort
    //
    public AvgAgeBuffer merge(AvgAgeBuffer buff1, AvgAgeBuffer buff2) {

        buff1.setTotal( buff1.getTotal() + buff2.getTotal() );
        buff1.setCnt( buff1.getCnt() + buff2.getCnt() );

        return buff1;
    }

    @Override
    // TODO finish : 完成计算
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

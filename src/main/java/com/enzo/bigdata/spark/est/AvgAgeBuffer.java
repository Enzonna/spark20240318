package com.enzo.bigdata.spark.est;

import scala.Serializable;

public class AvgAgeBuffer implements Serializable {
    private Long total;
    private Integer cnt;

    public AvgAgeBuffer(Long total, Integer cnt) {
        this.total = total;
        this.cnt = cnt;
    }

    public AvgAgeBuffer() {
    }

    public Long getTotal() {
        return total;
    }

    public void setTotal(Long total) {
        this.total = total;
    }

    public Integer getCnt() {
        return cnt;
    }

    public void setCnt(Integer cnt) {
        this.cnt = cnt;
    }
}

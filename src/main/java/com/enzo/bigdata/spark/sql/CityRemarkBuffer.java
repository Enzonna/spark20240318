package com.enzo.bigdata.spark.sql;

import java.io.Serializable;
import java.util.Map;

/**
 * @Classname CityRemarkBuffer
 * @Description TODO
 * @Date 2024/6/26 8:48
 * @Created by Enzo
 */
public class CityRemarkBuffer implements Serializable {
    private Long total;
    private Map<String,Long> cityMap;

    public CityRemarkBuffer(Long total, Map<String, Long> cityMap) {
        this.total = total;
        this.cityMap = cityMap;
    }

    public CityRemarkBuffer() {
    }

    public Long getTotal() {
        return total;
    }

    public void setTotal(Long total) {
        this.total = total;
    }

    public Map<String, Long> getCityMap() {
        return cityMap;
    }

    public void setCityMap(Map<String, Long> cityMap) {
        this.cityMap = cityMap;
    }
}

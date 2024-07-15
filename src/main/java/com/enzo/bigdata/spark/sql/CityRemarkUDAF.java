package com.enzo.bigdata.spark.sql;

import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.expressions.Aggregator;
import org.jetbrains.annotations.NotNull;

import java.util.*;

/**
 * @Classname CityRemarkUDAF
 * @Description TODO
 * @Date 2024/6/26 8:46
 * @Created by Enzo
 */
public class CityRemarkUDAF extends Aggregator<String, CityRemarkBuffer, String> {
    @Override
    public CityRemarkBuffer zero() {
        return new CityRemarkBuffer(0L, new HashMap<String, Long>());
    }

    @Override
    public CityRemarkBuffer reduce(CityRemarkBuffer buff, String city) {
        buff.setTotal(buff.getTotal() + 1);

        Map<String, Long> cityMap = buff.getCityMap();
        Long cnt = cityMap.get(city);
        if (cnt == null) {
            cityMap.put(city, 1L);
        } else {
            cityMap.put(city, cnt + 1);
        }
        buff.setCityMap(cityMap);
        return buff;
    }

    @Override
    public CityRemarkBuffer merge(CityRemarkBuffer buff1, CityRemarkBuffer buff2) {
        buff1.setTotal(buff1.getTotal() + buff2.getTotal());
        Map<String, Long> cityMap1 = buff1.getCityMap();
        Map<String, Long> cityMap2 = buff2.getCityMap();

        for (String key2 : cityMap2.keySet()) {
            Long cnt1 = cityMap1.get(key2);
            Long cnt2 = cityMap2.get(key2);
            if (cnt1 == null) {
                cityMap1.put(key2, cnt2);
            } else {
                cityMap1.put(key2, cnt1 + cnt2);
            }
        }
        buff1.setCityMap(cityMap1);

        return buff1;
    }

    @Override
    public String finish(CityRemarkBuffer buff) {
        Long totalCount = buff.getTotal();
        Map<String, Long> cityMap = buff.getCityMap();

        List<CityCount> ccs = new ArrayList<>();

        cityMap.forEach(
                (k, v) -> {
                    ccs.add(new CityCount(k, v));
                }
        );
        Collections.sort(ccs);

        StringBuilder builder = new StringBuilder();

        CityCount cityCount1 = ccs.get(0);
        CityCount cityCount2 = ccs.get(1);

        long p1 = cityCount1.getCount() * 100 / totalCount;
        long p2 = cityCount2.getCount() * 100 / totalCount;
        builder.append(cityCount1.getCity()).append(" ").append(p1).append("%");
        builder.append(",").append(cityCount2.getCity()).append(" ").append(p2).append("%");

        if (ccs.size() > 2) {
            builder.append(", 其他 ").append(100 - p2 - p1).append("%");
        }

        return builder.toString();
    }

    @Override
    public Encoder<CityRemarkBuffer> bufferEncoder() {
        return Encoders.bean(CityRemarkBuffer.class);
    }

    @Override
    public Encoder<String> outputEncoder() {
        return Encoders.STRING();
    }
}


class CityCount implements Comparable<CityCount> {
    private String city;
    private Long count;

    public CityCount() {
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }

    public CityCount(String city, Long count) {
        this.city = city;
        this.count = count;
    }


    @Override
    public int compareTo(@NotNull CityCount o) {
        return -(int) (this.count - o.count);
    }
}
package com.cn.base;

import java.sql.Timestamp;

public class UrlViewCount {
    public String url;
    public Long count;
    public Long startTime;
    public Long endTime;

    public UrlViewCount() {
    }

    public UrlViewCount(String url, Long count, Long startTime, Long endTime) {
        this.url = url;
        this.count = count;
        this.startTime = startTime;
        this.endTime = endTime;
    }

    @Override
    public String toString() {
        return "UrlViewCount{" +
                "url='" + url + '\'' +
                ", count=" + count +
                ", startTime=" + new Timestamp(startTime) +
                ", endTime=" + new Timestamp(endTime) +
                '}';
    }
}

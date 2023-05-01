package com.cn.pvuv.entity;

/**
 * pv uv 的统计结果实体，可用来输出到ES 或者 MySQL 等外部存储系统中
 */
public class RealStatResult {
    // 事件ID
    private String eventId ;
    // 统计时间
    private String statisticsTime ;
    // 统计类型 pv | uv | deviceUv
    private String statType ;
    // 数据类型 1：全量  2：单位时间内（5分钟 | 30分钟 | 60分钟）
    private String dataType ;
    // 5 | 30 | 60
    private int step ;
    // 统计的结果
    private Long count ;

    public String getStatisticsTime() {
        return statisticsTime;
    }

    public void setStatisticsTime(String statisticsTime) {
        this.statisticsTime = statisticsTime;
    }

    public String getStatType() {
        return statType;
    }

    public void setStatType(String statType) {
        this.statType = statType;
    }

    public String getDataType() {
        return dataType;
    }

    public void setDataType(String dataType) {
        this.dataType = dataType;
    }

    public int getStep() {
        return step;
    }

    public void setStep(int step) {
        this.step = step;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    @Override
    public String toString() {
        return "RealStatResult{" +
                "eventId='" + eventId + '\'' +
                ", statisticsTime='" + statisticsTime + '\'' +
                ", statType='" + statType + '\'' +
                ", dataType='" + dataType + '\'' +
                ", step=" + step +
                ", count=" + count +
                '}';
    }
}

package com.cn.pro;

public class StatResult {

    private String eventId;
    private String statisticsTime;
    private String statType;
    private String dataType;
    private int step;
    private Long count;


    public StatResult(String eventId, String statisticsTime, String statType, String dataType, int step, Long count) {
        this.eventId = eventId;
        this.statisticsTime = statisticsTime;
        this.statType = statType;
        this.dataType = dataType;
        this.step = step;
        this.count = count;
    }
}

package com.cn.pro;

import java.sql.Timestamp;

public class outEntity {

    private String eventId;
    private Long pv;
    private String startTime;


    public outEntity() {

    }

    public outEntity(String eventId, Long pv, String startTime) {
        this.eventId = eventId;
        this.pv = pv;
        this.startTime = startTime;
    }

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public Long getPv() {
        return pv;
    }

    public void setPv(Long pv) {
        this.pv = pv;
    }

    public String getStartTime() {
        return startTime;
    }

    public void setStartTime(String startTime) {
        this.startTime = startTime;
    }

    @Override
    public String toString() {
        return "outEntity{" +
                "eventId='" + eventId + '\'' +
                ", pv=" + pv +
                ", startTime='" + startTime + '\'' +
                '}';
    }
}

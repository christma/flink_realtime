package com.cn.pro;

public class RealPvEntity {
    private String eventId;
    private long timestamp;

    public RealPvEntity() {
    }

    public RealPvEntity(String eventId, long timestamp) {
        this.eventId = eventId;
        this.timestamp = timestamp;
    }
}

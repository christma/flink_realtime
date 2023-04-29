package com.cn.pro;

public class RealUvEntity {
    private String id;
    private String eventId;
    private long timestamp;

    public RealUvEntity() {
    }

    public RealUvEntity(String id, String eventId, long timestamp) {
        this.id = id;
        this.eventId = eventId;
        this.timestamp = timestamp;
    }
}

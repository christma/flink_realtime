package com.cn.pvuv.entity;

/**
 * 进行pv计算的数据实体
 */
public class RealStatPvEntity {
    // 事件ID
    private String eventId ;
    // 事件上报时间
    private long timestamp ;

    public RealStatPvEntity(String eventId, long timestamp) {
        this.eventId = eventId;
        this.timestamp = timestamp;
    }

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

}

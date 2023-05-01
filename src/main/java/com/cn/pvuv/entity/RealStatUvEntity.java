package com.cn.pvuv.entity;

/**
 * 进行uv计算的数据实体
 */
public class RealStatUvEntity {
    // 需要去重的指标对应的ID值  （userId | deviceId）
    private String id ;
    // 事件ID
    private String eventId ;
    // 事件上报时间
    private long timestamp ;
    // 通过id计算的分区数，方便后续keyBy处理
    private Integer partition ;

    public RealStatUvEntity(String id, String eventId, long timestamp) {
        this.id = id;
        this.eventId = eventId;
        this.timestamp = timestamp;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
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

    public Integer getPartition() {
        return partition;
    }

    public void setPartition(Integer partition) {
        this.partition = partition;
    }
}

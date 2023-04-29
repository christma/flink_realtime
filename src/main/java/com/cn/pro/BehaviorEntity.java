package com.cn.pro;

public class BehaviorEntity {

    private String userId;
    private String deviceId;
    private String eventId;
    private String visitDateTime;

    public BehaviorEntity() {
    }

    public BehaviorEntity(String userId, String deviceId, String eventId, String visitDateTime) {
        this.userId = userId;
        this.deviceId = deviceId;
        this.eventId = eventId;
        this.visitDateTime = visitDateTime;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getDeviceId() {
        return deviceId;
    }

    public void setDeviceId(String deviceId) {
        this.deviceId = deviceId;
    }

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public String getVisitDateTime() {
        return visitDateTime;
    }

    public void setVisitDateTime(String visitDateTime) {
        this.visitDateTime = visitDateTime;
    }

    @Override
    public String toString() {
        return "BehaviorEntity{" +
                "userId='" + userId + '\'' +
                ", deviceId='" + deviceId + '\'' +
                ", eventId='" + eventId + '\'' +
                ", visitDateTime='" + visitDateTime + '\'' +
                '}';
    }
}

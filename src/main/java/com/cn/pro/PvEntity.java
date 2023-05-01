package com.cn.pro;

import java.sql.Timestamp;

public class PvEntity {
    private String userId;
    private Timestamp startWindow;
    private Timestamp endWindow;
    private Long pv;


    public PvEntity() {
    }

    public PvEntity(String userId, Timestamp startWindow, Timestamp endWindow, Long pv) {
        this.userId = userId;
        this.startWindow = startWindow;
        this.endWindow = endWindow;
        this.pv = pv;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public Timestamp getStartWindow() {
        return startWindow;
    }

    public void setStartWindow(Timestamp startWindow) {
        this.startWindow = startWindow;
    }

    public Timestamp getEndWindow() {
        return endWindow;
    }

    public void setEndWindow(Timestamp endWindow) {
        this.endWindow = endWindow;
    }

    public Long getPv() {
        return pv;
    }

    public void setPv(Long pv) {
        this.pv = pv;
    }

    @Override
    public String toString() {
        return "PvEntity{" +
                "userId='" + userId + '\'' +
                ", startWindow='" + startWindow + '\'' +
                ", endWindow='" + endWindow + '\'' +
                ", pv=" + pv +
                '}';
    }
}

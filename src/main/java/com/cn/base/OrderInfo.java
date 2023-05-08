package com.cn.base;

import java.sql.Timestamp;

public class OrderInfo {
    private Integer id;
    private String gender;
    private Integer age;
    private Long price;
    private String os;
    private Long ts;


    public OrderInfo() {
    }

    public OrderInfo(Integer id, String gender, Integer age, Long price, String os, Long ts) {
        this.id = id;
        this.gender = gender;
        this.age = age;
        this.price = price;
        this.os = os;
        this.ts = ts;
    }

    public  Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getGender() {
        return gender;
    }

    public void setGender(String gender) {
        this.gender = gender;
    }

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }

    public Long getPrice() {
        return price;
    }

    public void setPrice(Long price) {
        this.price = price;
    }

    public String getOs() {
        return os;
    }

    public void setOs(String os) {
        this.os = os;
    }

    public Long getTs() {
        return ts;
    }

    public void setTs(Long ts) {
        this.ts = ts;
    }

    @Override
    public String toString() {
        return "OrderInfo{" +
                "id=" + id +
                ", gender='" + gender + '\'' +
                ", age=" + age +
                ", price=" + price +
                ", os='" + os + '\'' +
                ", ts=" + new Timestamp(ts) +
                '}';
    }


}

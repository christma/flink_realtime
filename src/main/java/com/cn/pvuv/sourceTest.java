package com.cn.pvuv;

import com.cn.pvuv.entity.BehaviorEntity;
import com.cn.pvuv.function.BehaviorSourceFunction;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.LocalTime;

public class sourceTest {
    public static void main(String[] args) throws Exception {


        Time step = Time.minutes(5);
        long stepMilliseconds = step.toMilliseconds();
        LocalTime startTime = LocalTime.of(0, 0, 0);
        long seconds = step.toMilliseconds() / 1000 ;
        LocalTime endTime = startTime.minusSeconds(seconds) ;
        int day = startTime.toSecondOfDay();
        int key = endTime.toSecondOfDay();
        System.out.println(day);
        System.out.println(stepMilliseconds);
        System.out.println(startTime);
        System.out.println(endTime);
        System.out.println(key);
    }
}

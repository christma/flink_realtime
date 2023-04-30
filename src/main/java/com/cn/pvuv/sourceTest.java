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
//        LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);
//
//        DataStreamSource<BehaviorEntity> source = env.addSource(new BehaviorSourceFunction());
//
//
//        source.print();
//
//
//        env.execute();

        Time step = Time.minutes(5);
        long stepMilliseconds = step.toMilliseconds();
//        LocalTime startTime = LocalTime.of(0, 0, 0);
//        long seconds = step.toMilliseconds() / 1000;
//        LocalTime endTime = startTime.minusSeconds(seconds);
//
//        System.out.println(startTime.toSecondOfDay());
//        System.out.println(startTime);
//        System.out.println(seconds);
//        System.out.println(endTime);
//        System.out.println(startTime.isBefore(endTime));
//        System.out.println(endTime.toSecondOfDay());
//        System.out.println(startTime.plusSeconds(seconds));

        long currentTimeMillis = System.currentTimeMillis();
        System.out.println( new Timestamp(currentTimeMillis/stepMilliseconds*stepMilliseconds));

    }
}

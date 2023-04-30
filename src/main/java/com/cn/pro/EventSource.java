package com.cn.pro;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Random;

public class EventSource extends RichSourceFunction<BehaviorEntity> {


    private Boolean flag = true;

    @Override
    public void run(SourceContext<BehaviorEntity> ctx) throws Exception {

        while (flag) {
            ctx.collect(new BehaviorEntity(getUserId(), getDeviceId(), getEventId(), getVisitTime()));
            Thread.sleep(100);
        }
    }

    @Override
    public void cancel() {
        flag = false;
    }

    public static String getUserId() {
        String[] userIdList = {"A", "B", "C", "D", "E", "F", "G", "H", "I", "G", "K", "L", "M", "N", "O", "P", "Q", "R", "S", "T", "U"};
        Random random = new Random();
        return userIdList[random.nextInt(userIdList.length - 1)];
    }

    public static String getDeviceId() {
        String[] deviceIdList = {"PC", "IOS", "ANDROID"};
        Random random = new Random();
        return deviceIdList[random.nextInt(deviceIdList.length - 1)];
    }

    public static String getEventId() {
        // 0 登陆，1点击，2购物车，3下单
        Random random = new Random();
        return String.valueOf(random.nextInt(1));
    }

    public static Long getVisitTime() {
        return System.currentTimeMillis();

    }

    public static void main(String[] args) {
        Time step = Time.minutes(1);
        System.out.println(step.toMilliseconds());
        for (int i = 0; i < 10; i++) {
            Long time = getVisitTime();

            System.out.println(new Timestamp(time));
            System.out.println(new Timestamp(time / step.toMilliseconds() * step.toMilliseconds()));

        }

    }

}

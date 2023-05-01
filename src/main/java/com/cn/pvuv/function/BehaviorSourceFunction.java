package com.cn.pvuv.function;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import com.cn.pvuv.entity.BehaviorEntity;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Random;

public class BehaviorSourceFunction extends RichSourceFunction<BehaviorEntity> {
    private boolean isRunning ;

    // 获取一个随机数
    private final Random random = new Random() ;

    @Override
    public void open(Configuration parameters) throws Exception {
        isRunning = true ;
    }

    @Override
    public void run(SourceContext<BehaviorEntity> ctx) throws Exception {
        while(isRunning) {
            int index = random.nextInt(10000);
            String eventId = "EVENT_" + index ;
            index = random.nextInt(20000);
            String userId = "USER_ID_" + index ;
            index = random.nextInt(15000);
            String deviceId = "DEVICE_ID_" + index ;
            String visitDateTime = LocalDateTime.now().minusDays(1).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));

            Thread.sleep(random.nextInt(3) * 1000);

            ctx.collect(new BehaviorEntity(userId,deviceId,eventId, visitDateTime));
        }
    }

    @Override
    public void cancel() {
        isRunning = false ;
    }
}

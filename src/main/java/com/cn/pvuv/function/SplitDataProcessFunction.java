package com.cn.pvuv.function;

import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import com.cn.pvuv.entity.BehaviorEntity;
import com.cn.pvuv.entity.RealStatPvEntity;
import com.cn.pvuv.entity.RealStatUvEntity;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import static com.cn.pvuv.config.ParamConstant.*;

/**
 * 将输入的流，通过侧输出流，形成各个统计指标[pv uv deviceUv]需要的数据流
 */
public class SplitDataProcessFunction extends KeyedProcessFunction<String, BehaviorEntity, BehaviorEntity> {
    @Override
    public void processElement(BehaviorEntity value,
                               KeyedProcessFunction<String, BehaviorEntity, BehaviorEntity>.Context ctx,
                               Collector<BehaviorEntity> out) throws Exception {
        long timestamp = Timestamp
                .valueOf(LocalDateTime.parse(value.getVisitDateTime(), DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))).getTime();
        // 输出 userId 去重计算的数据实体
        ctx.output(UV_OUTPUT_TAG, new RealStatUvEntity(value.getUserId(),ctx.getCurrentKey(),timestamp));

        // 输出deviceId 去重计算的数据实体
        ctx.output(DEVICE_UV_OUTPUT_TAG, new RealStatUvEntity(value.getDeviceId(),ctx.getCurrentKey(),timestamp));

        // 输出pv计算的数据实体
        ctx.output(PV_OUTPUT_TAG, new RealStatPvEntity(value.getEventId(), timestamp));

    }
}

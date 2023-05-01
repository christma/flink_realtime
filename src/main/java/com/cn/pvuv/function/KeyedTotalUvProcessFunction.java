package com.cn.pvuv.function;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import com.cn.pvuv.entity.RealStatResult;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

import static com.cn.pvuv.config.ParamConstant.*;

/**
 * 汇总所有的uv数据，输出最终的uv结果，输出到外部存储中
 */
public class KeyedTotalUvProcessFunction extends KeyedProcessFunction<String, RealStatResult,RealStatResult> {

    // UK: 统计时间    UV: k: 数据类型组合  1-5  2-5  2-30  2-60    v:统计结果
    private MapState<Long, Map<String, RealStatResult>> mapState ;

    private final Time step = Time.minutes(5) ;

    @Override
    public void open(Configuration parameters) throws Exception {
        mapState = getRuntimeContext().getMapState(UV_RESULT_MAP_STATE_DESC) ;
    }

    @Override
    public void processElement(RealStatResult value,
                               KeyedProcessFunction<String, RealStatResult, RealStatResult>.Context ctx,
                               Collector<RealStatResult> out) throws Exception {

        // 获取统计时间
        String statTime = value.getStatisticsTime();
        long statTimestamp = Timestamp.valueOf(LocalDateTime.parse(statTime, DateTimeFormatter.ofPattern(DATE_PATTERN))).getTime();

        String dataType = value.getDataType();
        int step = value.getStep();

        if(!mapState.contains(statTimestamp)) {
            ctx.timerService().registerEventTimeTimer(statTimestamp + this.step.toMilliseconds() - 1L);
            mapState.put(statTimestamp, new HashMap<>());
        }

        String key = dataType + "\003" + step ;
        Map<String, RealStatResult> dataMap = mapState.get(statTimestamp);
        if(!dataMap.containsKey(key)) {
            dataMap.put(key, value) ;
        }else {
            RealStatResult existResult = dataMap.get(key);
            existResult.setCount(existResult.getCount() + value.getCount());
        }
    }

    @Override
    public void onTimer(long timestamp,
                        KeyedProcessFunction<String, RealStatResult, RealStatResult>.OnTimerContext ctx,
                        Collector<RealStatResult> out) throws Exception {
        long key = timestamp + 1L - step.toMilliseconds() ;
        if(mapState.contains(key)) {
            mapState.get(key).values().forEach(out::collect);
            mapState.remove(key);
        }
    }
}

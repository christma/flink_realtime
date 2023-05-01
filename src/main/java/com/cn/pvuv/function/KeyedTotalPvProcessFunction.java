package com.cn.pvuv.function;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import com.cn.pvuv.entity.RealStatResult;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import static com.cn.pvuv.config.ParamConstant.*;

/**
 * 根据统计时间 keyBy 后，汇总各个统计时间点的统计结果，形成最终结果并且输出到外部存储中
 */
public class KeyedTotalPvProcessFunction extends KeyedProcessFunction<String, RealStatResult,RealStatResult> {

    private MapState<String,Long> mapState ;
    private ValueState<Boolean> hasCalFlagState ;


    private final Time step = Time.minutes(5) ;

    @Override
    public void open(Configuration parameters) throws Exception {
        mapState = getRuntimeContext().getMapState(TOTAL_PV_MAP_STATE_DESC) ;
        hasCalFlagState = getRuntimeContext().getState(HAS_CAL_VALUE_STATE_DESC) ;
    }

    @Override
    public void processElement(RealStatResult value,
                               KeyedProcessFunction<String, RealStatResult, RealStatResult>.Context ctx,
                               Collector<RealStatResult> out) throws Exception {
        String statTime = ctx.getCurrentKey();
        long statTimestamp = Timestamp.valueOf(LocalDateTime.parse(statTime, DateTimeFormatter.ofPattern(DATE_PATTERN))).getTime();

        // 注册定时器
        ctx.timerService().registerEventTimeTimer(statTimestamp + step.toMilliseconds() - 1L);

        value.setEventId("");
        value.setDataType(value.getDataType().equals(SINGLE_EVENT_TOTAL_) ? TOTAL_ : TOTAL_PER_MIN_);
        String key = value.getStatType() + "\003" + value.getDataType() + "\003" + value.getStep() ;
        if(mapState.contains(key)) {
            mapState.put(key, mapState.get(key) + value.getCount());
        }else {
            mapState.put(key, value.getCount());
        }

        if(hasCalFlagState.value() == null) {
            hasCalFlagState.update(false);
        }
    }

    @Override
    public void onTimer(long timestamp,
                        KeyedProcessFunction<String, RealStatResult, RealStatResult>.OnTimerContext ctx,
                        Collector<RealStatResult> out) throws Exception {
        if(!hasCalFlagState.value()) {
            hasCalFlagState.update(true);

            for (String key : mapState.keys()) {

                String[] split = key.split("\003");
                int step = Integer.parseInt(split[2]) ;
                String statType = split[0] ;
                String dataType = split[1] ;

                Long count = mapState.get(key);
                RealStatResult data = new RealStatResult() ;
                data.setEventId("");
                data.setCount(count);
                data.setStep(step);
                data.setDataType(dataType);
                data.setStatType(statType);
                data.setStatisticsTime(ctx.getCurrentKey());
                if(count != 0) {
                    out.collect(data);
                }
            }

            mapState.clear();
        }


    }
}

package com.cn.pvuv.function;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import com.cn.pvuv.entity.RealStatPvEntity;
import com.cn.pvuv.entity.RealStatResult;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import static com.cn.pvuv.config.ParamConstant.*;

/**
 * 根据eventId 进行keyBy 后，计算每个key下的pv数
 * 每5分钟统计当天总pv数
 * 每5分钟统计前5分钟内的数据pv
 * 每30分钟统计前30分钟内的数据pv
 * 每60分钟统计前60分钟内的数据pv
 */
public class KeyedPvProcessFunction extends KeyedProcessFunction<String, RealStatPvEntity, RealStatResult> {

    // 每5分钟一个时间点    前5分钟的统计结果
    // UK: 窗口的时间点    UV：对应的统计结果 pv值
    private MapState<Long, Long> mapState ;
    // 当前窗口所属日期的时间戳 按天划分窗口
    private ValueState<Long> currentWindowStart ;
    // 存储当前计算的时间节点对应的时间戳，一定是mapState中某个UK的值
    private ValueState<Long> currentComputationTime ;

    // 每5分钟输出一个统计结果
    private final Time step = Time.minutes(5) ;

    @Override
    public void open(Configuration parameters) throws Exception {
        // 初始化窗口状态
        mapState = getRuntimeContext().getMapState(PV_MAP_STATE_DESC) ;
        // 初始化当前所属窗口的状态
        currentWindowStart = getRuntimeContext().getState(CURRENT_WINDOW_STATE_DESC) ;
        // 初始化当前计算节点的状态
        currentComputationTime = getRuntimeContext().getState(CURRENT_COMPUTATION_TIME_STATE_DESC) ;
    }

    @Override
    public void processElement(RealStatPvEntity value,
                               KeyedProcessFunction<String, RealStatPvEntity, RealStatResult>.Context ctx,
                               Collector<RealStatResult> out) throws Exception {
        // 获取当前数据的访问时间
        long timestamp = value.getTimestamp();
        // 如果 mapState 是空的，意味着是第一条数据到来，或者第二天的第一条数据到来，需要初始化窗口信息
        if(mapState.isEmpty()) {
            // 获取该天的初始时刻对应的时间
            LocalDateTime currentDayMin = new Timestamp(timestamp).toLocalDateTime().with(LocalTime.MIN);
            // 初始化 mapState
            initMapState(currentDayMin,ctx);
            // 更新当前窗口的起始时间
            currentWindowStart.update(Timestamp.valueOf(currentDayMin).getTime());
            // 更新当前计算的时间
            currentComputationTime.update(Timestamp.valueOf(currentDayMin).getTime());
        }

        long currentWatermark = ctx.timerService().currentWatermark() ;

        // 判断是否跨天
        boolean overDay = false ;
        // 因为是周期性生成水印，当最开始的数据过来，水印还没有生成的时候，获取到的水印时间是Long的最小值，当程序终止的时候，水印时间会推到Long的最大值
        if(currentWatermark != Long.MIN_VALUE && currentWatermark != Long.MAX_VALUE) {
            long watermarkDayMin = Timestamp.valueOf(new Timestamp(currentWatermark).toLocalDateTime().with(LocalTime.MIN)).getTime();
            if(watermarkDayMin > currentWindowStart.value()) {
                // 跨天了
                overDay = true ;
                // 获取该天的初始时刻对应的时间
                LocalDateTime currentDayMin = new Timestamp(watermarkDayMin).toLocalDateTime().with(LocalTime.MIN);
                currentWindowStart.update(watermarkDayMin);
                completeMapState(currentDayMin,ctx);
            }
        }

        if(overDay) {
            // 清空状态
            Iterator<Long> iterator = mapState.keys().iterator();
            List<Long> mapStateKeys = new ArrayList<>() ;
            while(iterator.hasNext()) {
                mapStateKeys.add(iterator.next()) ;
            }
            for (Long key : mapStateKeys) {
                if(key < currentWindowStart.value()) {
                    mapState.remove(key);
                }
            }
        }

        // 将状态更新
        long currentMapKey = getCurrentMapKey(timestamp) ;
        if(currentMapKey >= currentWindowStart.value()) {
            Long pv = mapState.get(currentMapKey);
            if(pv == null) {
                pv = 0L ;
            }
            mapState.put(currentMapKey, ++pv);
        }
    }

    @Override
    public void onTimer(long timestamp,
                        KeyedProcessFunction<String, RealStatPvEntity, RealStatResult>.OnTimerContext ctx,
                        Collector<RealStatResult> out) throws Exception {
        // 获取统计时间
        long computationTime = timestamp - step.toMilliseconds() + 1L ;
        LocalDateTime localDateTime = new Timestamp(computationTime).toLocalDateTime();

        String statTime = localDateTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));

        // pv计算并且输出统计结果
        pvStat(ctx.getCurrentKey(),statTime, computationTime,localDateTime, out) ;

    }

    private void pvStat(String eventId,
                        String statTime,
                        long computationTime,
                        LocalDateTime localDateTime,
                        Collector<RealStatResult> out) throws Exception {
        // 定义结果集合
        List<Long> totalStatList = new ArrayList<>() ;
        for (Long key : mapState.keys()) {
            if(key <= computationTime) {
                Long pv = mapState.get(key);
                if(pv == null) continue;
                totalStatList.add(pv) ;
            }
        }
        // 统计此刻当天的累计结果
        stat(eventId,statTime,totalStatList,SINGLE_EVENT_TOTAL_,STEP_5_,out) ;

        // 获取5分钟内的统计结果
        totalStatList.clear();
        Long pv_5 = mapState.get(computationTime);
        totalStatList.add(pv_5) ;
        stat(eventId,statTime,totalStatList,SINGLE_EVENT_TOTAL_PER_MIN_,STEP_5_,out) ;

        // 获取30分钟内的统计结果
        totalStatList.clear();
        int computationMinutes = localDateTime.plusSeconds(step.toMilliseconds() / 1000).toLocalTime().getMinute();
        if(computationMinutes == 0 || computationMinutes == 30) {
            long preComputationTime = Timestamp.valueOf(localDateTime.minusMinutes(25L)).getTime();
            for (Long key : mapState.keys()) {
                if(key <= computationTime && key >= preComputationTime && mapState.get(key) != null) {
                    totalStatList.add(mapState.get(key)) ;
                }
            }
            stat(eventId,statTime,totalStatList,SINGLE_EVENT_TOTAL_PER_MIN_,STEP_30_,out) ;
        }

        // 获取60分钟内的统计结果
        totalStatList.clear();
        if(computationMinutes == 0) {
            long preComputationTime = Timestamp.valueOf(localDateTime.minusMinutes(55L)).getTime();
            for (Long key : mapState.keys()) {
                if(key <= computationTime && key >= preComputationTime && mapState.get(key) != null) {
                    totalStatList.add(mapState.get(key)) ;
                }
            }
            stat(eventId,statTime,totalStatList,SINGLE_EVENT_TOTAL_PER_MIN_,STEP_60_,out) ;
        }
    }
    // 封装统计结果
    private void stat(String eventId,
                      String statTime,
                      List<Long> totalStatList,
                      String dataType,
                      int step,
                      Collector<RealStatResult> out) {
        RealStatResult result = new RealStatResult() ;
        result.setEventId(eventId);
        result.setStep(step);
        result.setDataType(dataType);
        result.setStatType(STAT_TYPE_PV_);
        result.setStatisticsTime(statTime);
        Long totalPv = totalStatList.stream().filter(Objects::nonNull).reduce(Long::sum).orElse(null);
        if(totalPv != null && totalPv != 0L) {
            result.setCount(totalPv);
            out.collect(result);
        }
    }

    /**
     * 根据当前时间，解析处改时间所属key
     * @param timestamp 时间戳
     * @return
     */
    private long getCurrentMapKey(long timestamp) {
        LocalDateTime localDateTime = new Timestamp(timestamp).toLocalDateTime();
        LocalDateTime localMinTime = localDateTime.with(LocalTime.MIN);

        LocalTime localTime = localDateTime.toLocalTime();
        int seconds = localTime.toSecondOfDay();

        long stepSeconds = step.toMilliseconds() / 1000 ;
        return Timestamp.valueOf(localMinTime.plusSeconds((seconds / stepSeconds) * stepSeconds)).getTime() ;
    }


    /**
     * 对于第一条数据进来，初始化窗口状态
     * @param currentDayMin 第一条数据所属日期的开始时间
     * @param ctx 上下文对象
     * @throws Exception
     */
    private void initMapState(LocalDateTime currentDayMin,
                              KeyedProcessFunction<String, RealStatPvEntity, RealStatResult>.Context ctx) throws Exception{
        // 初始化 mapState
        LocalTime startTime = LocalTime.of(0, 0, 0);
        long seconds = step.toMilliseconds() / 1000 ;
        LocalTime endTime = startTime.minusSeconds(seconds) ;
        while(startTime.isBefore(endTime)) {
            int key = startTime.toSecondOfDay() ;
            mapState.put(Timestamp.valueOf(currentDayMin.plusSeconds(key)).getTime(), 0L);
            startTime = startTime.plusSeconds(seconds) ;
        }
        // 当天的最后一个key
        long key = endTime.toSecondOfDay() ;
        mapState.put(Timestamp.valueOf(currentDayMin.plusSeconds(key)).getTime(), 0L);
        // 为每一个计算时间点 注册一个定时器
        for (Long time : mapState.keys()) {
            ctx.timerService().registerEventTimeTimer(time + step.toMilliseconds() - 1L);
        }
    }

    /**
     * 第二天的第一条数据进来的时候，完善第二天的状态信息
     * @param currentDayMin 数据所属日期的开始时间
     * @param ctx 上下文对象
     * @throws Exception
     */
    private void completeMapState(LocalDateTime currentDayMin,
                              KeyedProcessFunction<String, RealStatPvEntity, RealStatResult>.Context ctx) throws Exception{
        // 初始化 mapState
        LocalTime startTime = LocalTime.of(0, 0, 0);
        long seconds = step.toMilliseconds() / 1000 ;
        LocalTime endTime = startTime.minusSeconds(seconds) ;
        while(startTime.isBefore(endTime)) {
            int key = startTime.toSecondOfDay() ;
            long mapKey = Timestamp.valueOf(currentDayMin.plusSeconds(key)).getTime();
            if(!mapState.contains(mapKey)) {
                mapState.put(mapKey, 0L);
            }
            ctx.timerService().registerEventTimeTimer(mapKey + step.toMilliseconds() - 1L);
            startTime = startTime.plusSeconds(seconds) ;
        }
        // 当天的最后一个key
        long key = endTime.toSecondOfDay() ;
        long mapKey = Timestamp.valueOf(currentDayMin.plusSeconds(key)).getTime();
        if(!mapState.contains(mapKey)) {
            mapState.put(mapKey, 0L);
        }
        ctx.timerService().registerEventTimeTimer(mapKey + step.toMilliseconds() - 1L);
    }
}

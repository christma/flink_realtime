package com.cn.pvuv.function;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import com.cn.pvuv.entity.RealStatResult;
import com.cn.pvuv.entity.RealStatUvEntity;
import com.cn.pvuv.entity.UvContainer;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;

import static com.cn.pvuv.config.ParamConstant.*;

public class KeyedUvProcessFunction extends KeyedProcessFunction<Integer, RealStatUvEntity, RealStatResult> {

    private final String statType ;

    private final Time step = Time.minutes(5) ;

    public KeyedUvProcessFunction(String statType) {
        this.statType = statType ;
    }

    // 存储当天累计的uv值
    private MapState<Long, UvContainer> mapState_total ;
    // 存储每5分钟内的数据uv值
    private MapState<Long, UvContainer> mapState_per5 ;
    // 存储每30分钟内的数据uv值
    private MapState<Long, UvContainer> mapState_per30 ;
    // 存储60分钟内的数据uv值
    private MapState<Long, UvContainer> mapState_per60 ;

    // 注册时间服务的记录
    private MapState<Long,Boolean> calFlagMapState ;

    @Override
    public void open(Configuration parameters) throws Exception {
        mapState_total = getRuntimeContext().getMapState(TOTAL_UV_MAP_STATE_DESC) ;
        mapState_per5 = getRuntimeContext().getMapState(PER5_UV_MAP_STATE_DESC) ;
        mapState_per30 = getRuntimeContext().getMapState(PER30_UV_MAP_STATE_DESC) ;
        mapState_per60 = getRuntimeContext().getMapState(PER60_UV_MAP_STATE_DESC) ;
        calFlagMapState = getRuntimeContext().getMapState(UV_CAL_FLAG_MAP_STATE_DESC) ;
    }

    @Override
    public void processElement(RealStatUvEntity value,
                               KeyedProcessFunction<Integer, RealStatUvEntity, RealStatResult>.Context ctx,
                               Collector<RealStatResult> out) throws Exception {
        // 获取当前数据的访问时间
        long timestamp = value.getTimestamp();

        boolean delay = false ;
        long currentWatermark = ctx.timerService().currentWatermark();
        // 判断是否是迟到数据
        if(currentWatermark != Long.MAX_VALUE && timestamp < currentWatermark) {
            delay = true ;
        }
        // 迟到数据不做计算
        if(delay) {
            return ;
        }
        // 更新状态
        long currentMapKey = getCurrentMapKey(timestamp);
        if(!mapState_per5.contains(currentMapKey)) {
            mapState_per5.put(currentMapKey, new UvContainer());
            LocalDateTime minTime = new Timestamp(timestamp).toLocalDateTime().with(LocalTime.MIN);
            if(!calFlagMapState.contains(Timestamp.valueOf(minTime).getTime())) {
                // 初始化状态
                calFlagMapState.put(Timestamp.valueOf(minTime).getTime(), true);

                // 为每个5分钟的时间节点注册一个定时器，用于触发计算
                LocalTime startTime = LocalTime.of(0, 0, 0);
                long seconds = step.toMilliseconds() / 1000 ;
                LocalTime endTime = startTime.minusSeconds(seconds) ;
                while(startTime.isBefore(endTime)) {
                    int secondOfDay = startTime.toSecondOfDay();
                    long key = Timestamp.valueOf(minTime.plusSeconds(secondOfDay)).getTime() ;
                    ctx.timerService().registerEventTimeTimer(key + step.toMilliseconds() - 1L);
                    startTime = startTime.plusSeconds(seconds) ;
                }
                int secondOfDay = endTime.toSecondOfDay();
                long key = Timestamp.valueOf(minTime.plusSeconds(secondOfDay)).getTime() ;
                ctx.timerService().registerEventTimeTimer(key + step.toMilliseconds() - 1L);
            }

            // 更新uv结果
            UvContainer uvContainer = mapState_per5.get(currentMapKey);
            uvContainer.getIds().add(value.getId()) ;
            mapState_per5.put(currentMapKey, uvContainer);
        }
    }


    @Override
    public void onTimer(long timestamp,
                        KeyedProcessFunction<Integer, RealStatUvEntity, RealStatResult>.OnTimerContext ctx,
                        Collector<RealStatResult> out) throws Exception {
        // 得到统计时间
        long computationTime = timestamp + 1L - step.toMilliseconds() ;
        LocalDateTime localDateTime = new Timestamp(computationTime).toLocalDateTime();
        String statTime = localDateTime.format(DateTimeFormatter.ofPattern(DATE_PATTERN));
        uvStat(statTime, computationTime, out) ;
    }

    /**
     * uv统计输出
     * @param statTime  统计时间 yyyy-MM-dd HH:mm:ss 格式
     * @param computationTime 统计时间戳
     * @param out 输出器
     */
    private void uvStat(String statTime, long computationTime, Collector<RealStatResult> out) throws Exception {
        // 当前统计时间 前5分钟的数据结果输出
        UvContainer uvContainer = mapState_per5.get(computationTime);
        if(uvContainer == null) {
            uvContainer = new UvContainer() ;
        }
        // 输出前5分钟内的数据uv
        stat(statTime, uvContainer, TOTAL_PER_MIN_,STEP_5_,out) ;
        mapState_per5.remove(computationTime);

        // 输出累计 | 30分钟 | 60分钟内的数据uv
        int minutes = Integer.parseInt(statTime.split(" ")[1].split(":")[1]) ;
        String statTime_per30 = statTime.split(":")[0] ;
        String statTime_per60 = statTime.split(":")[0] + ":55:00" ;
        String statTime_total = statTime.split(" ")[0] + " 00:00:00" ;
        if(minutes >= 0 && minutes <= 25) {
            statTime_per30 = statTime_per30 + ":25:00" ;
        }else {
            statTime_per30 = statTime_per30 + ":55:00" ;
        }
        // 求出对应的统计时间节点
        long computationTime_per30 = Timestamp.valueOf(LocalDateTime.parse(statTime_per30, DateTimeFormatter.ofPattern(DATE_PATTERN))).getTime();
        long computationTime_per60 = Timestamp.valueOf(LocalDateTime.parse(statTime_per60, DateTimeFormatter.ofPattern(DATE_PATTERN))).getTime();
        long computationTime_total = Timestamp.valueOf(LocalDateTime.parse(statTime_total, DateTimeFormatter.ofPattern(DATE_PATTERN))).getTime();

        // 合并到累计状态中
        if(!mapState_total.contains(computationTime_total)) {
            mapState_total.clear();
            mapState_total.put(computationTime_total, uvContainer);
            stat(statTime, uvContainer, TOTAL_ ,STEP_5_, out) ;
        }else {
            UvContainer existContainer = mapState_total.get(computationTime_total);
            // 将当前5分钟的结果merge到累计总量中去
            merge(existContainer, uvContainer) ;
            mapState_total.put(computationTime_total, existContainer);
            stat(statTime, existContainer, TOTAL_ ,STEP_5_, out) ;
        }

        // 合并到30分钟内的状态中
        if(!mapState_per30.contains(computationTime_per30)) {
            if(!mapState_per30.isEmpty()) {
                Map.Entry<Long, UvContainer> preData = mapState_per30.iterator().next();
                UvContainer preUvContainer = preData.getValue();
                Long time = preData.getKey();
                String preStatTime = new Timestamp(time).toLocalDateTime().format(DateTimeFormatter.ofPattern(DATE_PATTERN));
                // 输出30分钟内的uv统计结果
                stat(preStatTime, preUvContainer, TOTAL_PER_MIN_ ,STEP_30_, out) ;
            }
            mapState_per30.clear();
            mapState_per30.put(computationTime_per30, uvContainer);
        }else {
            UvContainer existContainer = mapState_per30.get(computationTime_per30);
            merge(existContainer, uvContainer);
            mapState_per30.put(computationTime_per30, existContainer);
        }

        // 合并到60分钟内的状态中
        if(!mapState_per60.contains(computationTime_per60)) {
            if(!mapState_per60.isEmpty()) {
                Map.Entry<Long, UvContainer> preData = mapState_per60.iterator().next();
                UvContainer preUvContainer = preData.getValue();
                Long time = preData.getKey();
                String preStatTime = new Timestamp(time).toLocalDateTime().format(DateTimeFormatter.ofPattern(DATE_PATTERN));
                // 输出60分钟内的uv统计结果
                stat(preStatTime, preUvContainer, TOTAL_PER_MIN_ ,STEP_60_, out) ;
            }
            mapState_per60.clear();
            mapState_per60.put(computationTime_per60, uvContainer);
        }else {
            UvContainer existContainer = mapState_per60.get(computationTime_per60);
            merge(existContainer, uvContainer);
            mapState_per60.put(computationTime_per60, existContainer);
        }
    }

    /**
     * 将统计结果汇总输出
     * @param statTime  统计时间
     * @param uvContainer  需要汇总的uv数据
     * @param dataType   统计的结果数据类型  1： 累计  2：单位时间内
     * @param step   单位时间周期
     * @param out    输出器
     */
    private void stat(String statTime,
                      UvContainer uvContainer,
                      String dataType,
                      int step,
                      Collector<RealStatResult> out) {
        RealStatResult result = new RealStatResult() ;
        result.setEventId("");
        result.setStatType(statType);
        result.setDataType(dataType);
        result.setStatisticsTime(statTime);
        result.setStep(step);
        long count = uvContainer.getIds().size() ;
        result.setCount(count);
        if(count != 0) {
            out.collect(result);
        }
    }

    /**
     * 合并uv数据
     * @param existContainer 已存在的对象
     * @param uvContainer 待合并的数据对象
     */
    private void merge(UvContainer existContainer, UvContainer uvContainer) {
        existContainer.getIds().addAll(uvContainer.getIds()) ;
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
}

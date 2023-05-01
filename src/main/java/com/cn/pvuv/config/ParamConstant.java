package com.cn.pvuv.config;

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.util.OutputTag;
import com.cn.pvuv.entity.RealStatPvEntity;
import com.cn.pvuv.entity.RealStatResult;
import com.cn.pvuv.entity.RealStatUvEntity;
import com.cn.pvuv.entity.UvContainer;

import java.util.Map;

/**
 * 实时统计中的静态常量
 */
public class ParamConstant {
    // userId去重的uv值对应的侧输出标签
    public static final OutputTag<RealStatUvEntity> UV_OUTPUT_TAG = new OutputTag<RealStatUvEntity>("uv-output-tag"){} ;

    // deviceId去重的uv值对应的侧输出标签
    public static final OutputTag<RealStatUvEntity> DEVICE_UV_OUTPUT_TAG = new OutputTag<RealStatUvEntity>("device-uv-output-tag"){} ;

    // pv值对应的侧输出标签
    public static final OutputTag<RealStatPvEntity> PV_OUTPUT_TAG = new OutputTag<RealStatPvEntity>("pv-output-tag"){} ;

    // pv统计结果的状态描述器
    public static final MapStateDescriptor<Long, Long> PV_MAP_STATE_DESC = new MapStateDescriptor<>("pv-map-state-desc", Long.class, Long.class);

    public static final ValueStateDescriptor<Long> CURRENT_WINDOW_STATE_DESC = new ValueStateDescriptor<>("current-window-state-desc", Long.class);

    public static final ValueStateDescriptor<Long> CURRENT_COMPUTATION_TIME_STATE_DESC = new ValueStateDescriptor<>("current-computation-time-state-desc", Long.class);

    public static final MapStateDescriptor<String,Long> TOTAL_PV_MAP_STATE_DESC = new MapStateDescriptor<>("total-pv-map-state-desc", String.class, Long.class) ;

    public static final ValueStateDescriptor<Boolean> HAS_CAL_VALUE_STATE_DESC = new ValueStateDescriptor<>("has-cal-value-state-desc", Boolean.class) ;


    // uv值统计结果的状态描述器
    public static final MapStateDescriptor<Long, UvContainer> TOTAL_UV_MAP_STATE_DESC = new MapStateDescriptor<>("total-uv-map-state-desc", Long.class, UvContainer.class);
    public static final MapStateDescriptor<Long, UvContainer> PER5_UV_MAP_STATE_DESC = new MapStateDescriptor<>("per5-uv-map-state-desc", Long.class, UvContainer.class);
    public static final MapStateDescriptor<Long, UvContainer> PER30_UV_MAP_STATE_DESC = new MapStateDescriptor<>("per30-uv-map-state-desc", Long.class, UvContainer.class);
    public static final MapStateDescriptor<Long, UvContainer> PER60_UV_MAP_STATE_DESC = new MapStateDescriptor<>("per60-uv-map-state-desc", Long.class, UvContainer.class);
    public static final MapStateDescriptor<Long, Boolean> UV_CAL_FLAG_MAP_STATE_DESC = new MapStateDescriptor<>("uv-cal-flag-map-state-desc", Long.class, Boolean.class);

    public static final MapStateDescriptor<Long, Map<String, RealStatResult>> UV_RESULT_MAP_STATE_DESC = new MapStateDescriptor<>("uv-result-map-state-desc", Types.LONG, Types.MAP(Types.STRING, Types.POJO(RealStatResult.class))) ;

    // 表示是当天的总量统计结果（每5分钟当天全量的pv|uv结果）
    public static final String TOTAL_ = "1" ;
    // 表示间隔时间（5、30、60分钟）内pv|uv的结果
    public static final String TOTAL_PER_MIN_ = "2" ;
    // 表示单个事件当天的总量统计结果
    public static final String SINGLE_EVENT_TOTAL_ = "3" ;
    // 表示单个事件间隔时间（5、30、60分钟）内的pv|uv的统计结果
    public static final String SINGLE_EVENT_TOTAL_PER_MIN_ = "4" ;

    public static final int STEP_5_ = 5 ;
    public static final int STEP_30_ = 30 ;
    public static final int STEP_60_ = 60 ;

    public static final String STAT_TYPE_PV_ = "pv" ;
    public static final String STAT_TYPE_UV_ = "uv" ;
    public static final String STAT_TYPE_DEVICE_UV_ = "deviceUv" ;

    public static final String DATE_PATTERN = "yyyy-MM-dd HH:mm:ss" ;


}

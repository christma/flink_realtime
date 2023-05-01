package com.cn.pvuv.function;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.util.OutputTag;
import org.apache.http.HttpHost;
import com.cn.pvuv.config.ParamConstant;
import com.cn.pvuv.entity.BehaviorEntity;
import com.cn.pvuv.entity.RealStatPvEntity;
import com.cn.pvuv.entity.RealStatResult;
import com.cn.pvuv.entity.RealStatUvEntity;

import java.util.List;

/**
 * pv uv统计的主逻辑
 */
public class StatMain {
    /**
     * pv统计
     * @param dataStream 包含对应的侧输出流
     * @param pvOutputTag 需要计算的pv的侧输出流标签
     */
    public static void pvStat(SingleOutputStreamOperator<BehaviorEntity> dataStream,
                              OutputTag<RealStatPvEntity> pvOutputTag,
                              List<HttpHost> hostList,
                              String userName,
                              String password,
                              String indexPrefix){
        // 获取单个事件在各个时间节点的pv值
        SingleOutputStreamOperator<RealStatResult> singleEventPvResultStream = dataStream.getSideOutput(pvOutputTag)
                .keyBy(RealStatPvEntity::getEventId)
                .process(new KeyedPvProcessFunction())
                .name("single-event-" + pvOutputTag.getId())
                .uid("single-event-" + pvOutputTag.getId());


        // 计算总pv 在各个时间节点的pv 值
        singleEventPvResultStream.keyBy(RealStatResult::getStatisticsTime)
                .process(new KeyedTotalPvProcessFunction())
                .name("total-pv-process")
                .uid("total-pv-process")
//                .addSink(EsSinkFunctionFactory.getElasticsearchSinkFunction(hostList, userName, password, indexPrefix))
//                .filter(new FilterFunction<RealStatResult>() {
//                    @Override
//                    public boolean filter(RealStatResult value) throws Exception {
//                        return value.getDataType().equals(ParamConstant.TOTAL_);
//                    }
//                })
                .print("pv")
                .name("total-pv-sink")
                .uid("total-pv-sink") ;
    }


    /**
     * uv 统计
     * @param dataStream  数据流
     * @param uvOutputTag 侧输出标签
     * @param statType    统计类型  uv | deviceUv 。。。
     * @param partitionSize  分区大小，方便并行计算
     * @param hostList    输出的es的连接地址
     * @param userName    输出的es的连接用户名
     * @param password    输出的es的连接密码
     * @param indexPrefix 输出的es的索引前缀
     */
    public static void uvStat(SingleOutputStreamOperator<BehaviorEntity> dataStream,
                              OutputTag<RealStatUvEntity> uvOutputTag,
                              String statType,
                              int partitionSize,
                              List<HttpHost> hostList,
                              String userName,
                              String password,
                              String indexPrefix){
        // 获取单个事件在各个时间节点的pv值
        dataStream.getSideOutput(uvOutputTag)
                .map(new MapFunction<RealStatUvEntity, RealStatUvEntity>() {
                    @Override
                    public RealStatUvEntity map(RealStatUvEntity value) throws Exception {
                        value.setPartition(Math.abs(value.getId().hashCode() % partitionSize));
                        return value;
                    }
                })
                .keyBy(RealStatUvEntity::getPartition)
                .process(new KeyedUvProcessFunction(statType))
                .name(statType + "-single-event-" + uvOutputTag.getId())
                .uid(statType + "-single-event-" + uvOutputTag.getId())
                .keyBy(d -> d.getStatisticsTime().split(" ")[1])
                .process(new KeyedTotalUvProcessFunction())
                .name(statType + "-total-uv-process")
                .uid(statType + "-total-uv-process")
//                .addSink(EsSinkFunctionFactory.getElasticsearchSinkFunction(hostList, userName, password, indexPrefix))
                .filter(new FilterFunction<RealStatResult>() {
                    @Override
                    public boolean filter(RealStatResult value) throws Exception {
                        return value.getDataType().equals(ParamConstant.TOTAL_);
                    }
                })
                .print(statType)
                .name(statType + "-total-uv-sink")
                .uid(statType + "-total-uv-sink") ;
    }

}

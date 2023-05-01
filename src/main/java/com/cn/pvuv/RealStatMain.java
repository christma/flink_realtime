package com.cn.pvuv;

import com.cn.pvuv.config.ParamConstant;
import com.cn.pvuv.entity.BehaviorEntity;
import com.cn.pvuv.function.BehaviorSourceFunction;
import com.cn.pvuv.function.SplitDataProcessFunction;
import com.cn.pvuv.function.StatMain;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.http.HttpHost;


import java.sql.Timestamp;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

public class RealStatMain {

    public static void main(String[] args) throws Exception {
        // 配置初始化
        Configuration configuration = initConfig() ;
        // 获取流执行环境
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        // 生产上并行度自行调整
        env.setParallelism(1) ;
        // 设置checkpoint的执行周期
//        env.enableCheckpointing(6000) ;
//        // 设置checkpoint成功后，间隔多久发起下一次checkpoint
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(6000);
//        // 设置checkpoint的超时时间
//        env.getCheckpointConfig().setCheckpointTimeout(8000);
//        // 设置flink任务被手动取消时，将最近一次成功的checkpoint数据保存到savepoint中，以便下次启动时，从该状态中恢复
//        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//        // 设置状态后端,视情况而定，生产上一般使用RocksDB作为状态后端存储状态信息
//        env.setStateBackend(new FsStateBackend("file:/data/flink/checkpoint/", true)) ;
        // 添加数据源, 一般是从kafka消费数据，这里为了能直接运行，使用一个自定义的数据源
        DataStreamSource<BehaviorEntity> sourceStream = env.addSource(new BehaviorSourceFunction());

        // 添加水印
        SingleOutputStreamOperator<BehaviorEntity> dataStream = sourceStream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<BehaviorEntity>forBoundedOutOfOrderness(Duration.ofSeconds(5L))
                        .withTimestampAssigner(new SerializableTimestampAssigner<BehaviorEntity>() {
                            @Override
                            public long extractTimestamp(BehaviorEntity element, long recordTimestamp) {
                                String visitDateTime = element.getVisitDateTime();
                                return Timestamp
                                        .valueOf(LocalDateTime.parse(visitDateTime, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")))
                                        .getTime();
                            }
                        })
        );

        // 对数据源进行拆流，方便共用通用的uvProcessFunction
        SingleOutputStreamOperator<BehaviorEntity> splitStream = dataStream.keyBy(BehaviorEntity::getEventId)
                .process(new SplitDataProcessFunction())
                .name("split-task")
                .uid("split-task");

        List<HttpHost> hostList = new ArrayList<>() ;
        String userName = "" ;
        String password = "" ;
        String indexPrefix = "" ;

        // pv统计
        StatMain.pvStat(splitStream, ParamConstant.PV_OUTPUT_TAG, hostList, userName, password, indexPrefix);

         int partitionSize = 100 ;
        // uv统计
//         StatMain.uvStat(splitStream, ParamConstant.UV_OUTPUT_TAG, ParamConstant.STAT_TYPE_UV_,partitionSize, hostList, userName, password, indexPrefix);

        // deviceUv统计
//         StatMain.uvStat(splitStream, ParamConstant.DEVICE_UV_OUTPUT_TAG, ParamConstant.STAT_TYPE_DEVICE_UV_,partitionSize, hostList, userName, password, indexPrefix);


        env.execute() ;
    }

    private static Configuration initConfig() {
        // 当前的配置信息生产上是放在一个配置文件中的
        Configuration configuration = new Configuration() ;
        // 对于本地运行模式，设置WEB-UI访问的端口
        configuration.setInteger(RestOptions.PORT, 7001);
        // 设置checkpoint的执行间隔时间
//        configuration.setInteger("checkpoint.interval" , 6000);
//        // 设置checkpoint的保存路径  file: 表示本地路径（默认当前项目所在磁盘的根路径下）  hdfs:// 则是表示hdfs路径
//        configuration.setString("checkpoint.data.dir", "file:/data/flink/checkpoint/");
//        // 设置两次checkpoint之间必须等待的时间
//        configuration.setInteger("checkpoint.min.pause.checkpoints", 6000);
//        // 设置checkpoint的超时时间
//        configuration.setInteger("checkpoint.timeout" , 8000);

        return configuration;
    }
}

package com.cn.sql;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.hadoop.fs.FileSystem;

public class CumulateWindowSQLTest {
    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        env.setParallelism(1);
        env.disableOperatorChaining();

        // 设置checkpoint的执行周期
        env.enableCheckpointing(6000);
        // 设置checkpoint成功后，间隔多久发起下一次checkpoint
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(6000);
        // 设置checkpoint的超时时间
        env.getCheckpointConfig().setCheckpointTimeout(8000);
        // 设置flink任务被手动取消时，将最近一次成功的checkpoint数据保存到savepoint中，以便下次启动时，从该状态中恢复
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // 设置状态后端,视情况而定，生产上一般使用RocksDB作为状态后端存储状态信息
        env.setStateBackend(new FsStateBackend("hdfs://localhost:9000/flink/checkpoints", true));


        String kafka_source = "CREATE TABLE user_log (\n" +
                "  `id` BIGINT,\n" +
                "  `age` BIGINT,\n" +
                "  `gender` STRING,\n" +
                "  `os` STRING,\n" +
                "  `price` BIGINT,\n" +
                "  `ts` BIGINT,\n" +
                "  `time_ltz` AS TO_TIMESTAMP_LTZ(ts, 3),\n" +
                "  WATERMARK FOR time_ltz AS time_ltz - INTERVAL '5' SECOND \n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'mock',\n" +
                "  'properties.bootstrap.servers' = 'localhost:9092',\n" +
                "  'properties.group.id' = 'testGroup',\n" +
                "  'scan.startup.mode' = 'latest-offset',\n" +
                "  'format' = 'json'\n" +
                ")";

//        String mysql_dim = "";

        String print_sink = "create table print_table (" +
                "`window_start` timestamp(3)," +
                "`window_end` timestamp(3)," +
                " id bigint," +
                " cn bigint," +
                " amount bigint " +
                ")with(" +
                "'connector'='jdbc',\n" +
                "'driver' =  'com.mysql.cj.jdbc.Driver'," +
                "'url'='jdbc:mysql://localhost:3306/d1?characterEncoding=UTF-8&useUnicode=true&useSSL=false&tinyInt1isBit=false&allowPublicKeyRetrieval=true&serverTimezone=Asia/Shanghai',\n" +
                "'table-name'='cumulate_table',\n" +
                "'username'='root',\n" +
                "'password'='root'\n" +
                ")";


//        String print_sink = "create table print_table (" +
//                "`window_start` timestamp(3)," +
//                "`window_end` timestamp(3)," +
//                " id bigint," +
//                " amount bigint" +
//                ")with(" +
//                "'connector'='print'" +
//                ")";


        String comp = " insert into print_table \n" +
                "SELECT window_end,\n" +
                "       window_start,\n" +
                "       id,\n" +
                "       count(id) as cn,\n" +
                "       sum(price) as amount\n" +
                "FROM TABLE(CUMULATE(\n" +
                "    TABLE user_log\n" +
                "         , DESCRIPTOR(time_ltz)\n" +
                "    , INTERVAL '60' SECOND\n" +
                "    , INTERVAL '5' MINUTE))\n" +
                "GROUP BY window_start,\n" +
                "         window_end,\n" +
                "         id";


        tEnv.executeSql(kafka_source);
        tEnv.executeSql(print_sink);
        tEnv.executeSql(comp);


        env.execute("CumulateWindowSQLTest");

    }
}

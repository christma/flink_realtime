package com.cn.sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;


public class DUASQL {
    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);


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


        String print_sink = "create table print_table (" +
                "`window_start` timestamp(3)," +
                "`window_end` timestamp(3)," +
                "os string," +
                "dau bigint" +
                ")with(" +
                "'connector'='print'" +
                ")";

        String comp = "insert into print_table " +
                "    SELECT  \n" +
                "    window_start \n" +
                "    , window_end \n" +
                "    , platform \n" +
                "    , sum(bucket_dau) as dau\n" +
                "from (\n" +
                "    SELECT\n" +
                "         window_start\n" +
                "        , window_end\n" +
                "        , os as platform\n" +
                "        , count(distinct id) as bucket_dau\n" +
                "    FROM TABLE(\n" +
                "        CUMULATE(\n" +
                "        TABLE user_log,\n" +
                "        DESCRIPTOR(time_ltz),\n" +
                "        INTERVAL '10' SECOND\n" +
                "        , INTERVAL '1' DAY))\n" +
                "    GROUP BY                                  \n" +
                "        window_start\n" +
                "        , window_end\n" +
                "        , os \n" +
                "        , MOD(HASH_CODE(id), 10)\n" +
                ") tmp\n" +
                "GROUP by   \n" +
                "    window_start\n" +
                "    , window_end\n" +
                "    , platform";

        tEnv.executeSql(kafka_source);
        tEnv.executeSql(print_sink);
        tEnv.executeSql(comp);


        env.execute();
    }
}

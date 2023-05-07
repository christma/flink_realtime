package com.cn.sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class TopNTest {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);


        String source_table = "CREATE TABLE user_log (\n" +
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
                " id bigint," +
                " sum_price bigint," +
                " rk bigint," +
                " primary key (window_start,window_end,id) NOT ENFORCED\n" +
                ")with(" +
                "'connector'='jdbc',\n" +
                "'driver' =  'com.mysql.cj.jdbc.Driver'," +
                "'url'='jdbc:mysql://localhost:3306/d1?characterEncoding=UTF-8&useUnicode=true&useSSL=false&tinyInt1isBit=false&allowPublicKeyRetrieval=true&serverTimezone=Asia/Shanghai',\n" +
                "'table-name'='user_topn',\n" +
                "'username'='root',\n" +
                "'password'='root'\n" +
                ")";

        String comp = "insert into print_table\n" +
                "select window_start,window_end,id,sum_price,rk from (\n" +
                "    select window_start,window_end,id,sum_price,row_number() over (partition by window_start,window_end order by sum_price desc) as rk\n" +
                "from (select window_start, window_end, id,sum(price) as sum_price\n" +
                "      from table(tumble(table user_log, descriptor(time_ltz), interval '1' minutes))\n" +
                "      group by window_start, window_end, id) t0\n" +
                "              ) t1 where rk <=3";




        tEnv.executeSql(source_table);
        tEnv.executeSql(print_sink);
        tEnv.executeSql(comp);

        env.execute();


    }
}

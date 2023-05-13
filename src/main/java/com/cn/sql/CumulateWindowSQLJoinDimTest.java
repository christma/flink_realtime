package com.cn.sql;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class CumulateWindowSQLJoinDimTest {
    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(1);
        env.disableOperatorChaining();
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


        String mysql_dim = "create table id_dim (" +
                "`id` int," +
                "`name` string," +
                " primary key(id) not enforced " +
                ")with(" +
                "'connector'='mysql-cdc',\n" +
                "'hostname'= 'localhost'," +
                "'port'='3306',\n" +
                "'username'='root',\n" +
                "'password'='root'," +
                "'database-name'='d1',\n" +
                "'table-name'='id_dim'" +
                ")";

//        String mysql_dim = "create table id_dim (" +
//                "`id` int," +
//                "`name` string," +
//                " primary key(id) not enforced " +
//                ")with(" +
//                "'connector'='jdbc',\n" +
//                "'driver' =  'com.mysql.cj.jdbc.Driver'," +
//                "'url'='jdbc:mysql://localhost:3306/d1?characterEncoding=UTF-8&useUnicode=true&useSSL=false&tinyInt1isBit=false&allowPublicKeyRetrieval=true&serverTimezone=Asia/Shanghai',\n" +
//                "'table-name'='id_dim',\n" +
//                "'username'='root',\n" +
//                "'password'='root'," +
//                "'lookup.cache.max-rows'='-1',\n" +
//                "'lookup.cache.ttl'='500ms'" +
//                ")";

// "'lookup.cache.max-rows'='10',\n" +
//                "'lookup.cache.ttl'='10'," +
//                "'lookup.cache.caching-missing-key'='true'" +
        String print_sink = "create table print_table (" +
                " id bigint," +
                " name string," +
                " cn bigint," +
                " amount bigint," +
                "`window_start` timestamp(3)," +
                "`window_end` timestamp(3)," +
                " primary key(id,window_start,window_end) not enforced " +
                ")with(" +
                "'connector'='jdbc',\n" +
                "'driver' =  'com.mysql.cj.jdbc.Driver'," +
                "'url'='jdbc:mysql://localhost:3306/d1?characterEncoding=UTF-8&useUnicode=true&useSSL=false&tinyInt1isBit=false&allowPublicKeyRetrieval=true&serverTimezone=Asia/Shanghai',\n" +
                "'table-name'='cumulate_table',\n" +
                "'username'='root',\n" +
                "'password'='root'\n" +
                ")";

//
//        String print_sink = "create table print_table (" +
//                " id bigint," +
//                " name string," +
//                " cn bigint," +
//                " amount bigint," +
//                "`window_start` timestamp(3)," +
//                "`window_end` timestamp(3)" +
//                ")with(" +
//                "'connector'='print'" +
//                ")";


        String comp = " insert into print_table \n" +
                "select tmp.id,name,cn,amount,window_start,window_end from (SELECT window_start,\n" +
                "       window_end,\n" +
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
                "         id) tmp left join id_dim d on d.id = tmp.id";


        tEnv.executeSql(mysql_dim);
        tEnv.executeSql(kafka_source);
        tEnv.executeSql(print_sink);
        tEnv.executeSql(comp);


        env.execute("CumulateWindowSQLTest");

    }
}

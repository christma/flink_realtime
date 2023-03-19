package com.cn;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MySQL_CDC {
    public static void main(String[] args) throws Exception {


        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("localhost")
                .port(3306)
                .databaseList("flinktrain") // set captured database, If you need to synchronize the whole database, Please set tableList to ".*".
                .tableList("flinktrain.access") // set captured table
                .username("root")
                .password("root")
                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                .build();


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(3000);


        env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL_Source")
                .print();

        env.execute("MySQL_CDC");
    }
}
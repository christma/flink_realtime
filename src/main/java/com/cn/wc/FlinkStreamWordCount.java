package com.cn.wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class FlinkStreamWordCount {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> streamSource = env.readTextFile("input/words.txt");


        SingleOutputStreamOperator<Tuple2<String, Long>> wordAndOne = streamSource.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
            String[] words = line.split(" ");

            for (String word: words) {
                out.collect(Tuple2.of(word, 1L));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.LONG));


        KeyedStream<Tuple2<String, Long>, String> tuple2WordAndOneKeyedStream = wordAndOne.keyBy(word -> word.f0);

        SingleOutputStreamOperator<Tuple2<String, Long>> sum = tuple2WordAndOneKeyedStream.sum(1);

        sum.print();

        env.execute();


    }
}

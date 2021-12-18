package org.apache.flink.librariesplus.test;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ceppro.CEP;
import org.apache.flink.ceppro.PatternStream;
import org.apache.flink.ceppro.RichPatternSelectFunction;
import org.apache.flink.ceppro.pattern.Pattern;
import org.apache.flink.ceppro.pattern.conditions.RichIterativeCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author johnCai
 * @version 1.0
 * @date 2021/12/15 下午10:43
 */
public class CepTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

//		数据源
        SingleOutputStreamOperator<Tuple3<String, Long, String>> source = env.fromElements(
                new Tuple3<String, Long, String>("a", 1000000001000L, "22")
                , new Tuple3<String, Long, String>("b", 1000000002000L, "23")
                , new Tuple3<String, Long, String>("b", 1000000003000L, "23")
                , new Tuple3<String, Long, String>("c", 1000000004000L, "23")
                , new Tuple3<String, Long, String>("b", 1000000005000L, "23")
                , new Tuple3<String, Long, String>("d", 1000000005000L, "23")
                , new Tuple3<String, Long, String>("d", 1000000006000L, "23")
//			, new Tuple3<String, Long, String>("d", 1000000003000L, "23")
//			, new Tuple3<String, Long, String>("c", 1000000003001L, "23")
//			, new Tuple3<String, Long, String>("e", 1000000004000L, "24")
//			, new Tuple3<String, Long, String>("f", 1000000005000L, "23")
//			, new Tuple3<String, Long, String>("g", 1000000006000L, "23")
//			, new Tuple3<String, Long, String>("b", 1000000007000L, "23")
//			, new Tuple3<String, Long, String>("c", 1000000008000L, "23")
//			, new Tuple3<String, Long, String>("d", 1000000009000L, "23")
//			, new Tuple3<String, Long, String>("change", 1000000010001L, "23")
//			, new Tuple3<String, Long, String>("e", 1000000011000L, "24")
//			, new Tuple3<String, Long, String>("f", 1000000012000L, "23")
//			, new Tuple3<String, Long, String>("g", 1000000013000L, "23")
        ).assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<Tuple3<String, Long, String>>() {
            long maxTimsStamp;

            public Watermark checkAndGetNextWatermark(Tuple3<String, Long, String> stringLongStringTuple3, long l) {
                return new Watermark(maxTimsStamp);
            }

            public long extractTimestamp(Tuple3<String, Long, String> stringLongStringTuple3, long per) {
                long elementTime = stringLongStringTuple3.f1;
                if (elementTime > maxTimsStamp) {
                    maxTimsStamp = elementTime;
                }
                return elementTime;
            }
        });

        Pattern<Tuple3<String, Long, String>, ?> pattern = Pattern
                .<Tuple3<String, Long, String>>begin("start")
                .where(new RichIterativeCondition<Tuple3<String, Long, String>>() {
                    @Override
                    public boolean filter(Tuple3<String, Long, String> value, Context<Tuple3<String, Long, String>> ctx) throws Exception {
                        return value.f0.equals("a");
                    }
                })
                .next("middle")
                .where(new RichIterativeCondition<Tuple3<String, Long, String>>() {
                    @Override
                    public boolean filter(Tuple3<String, Long, String> value, Context<Tuple3<String, Long, String>> ctx) throws Exception {
                        return value.f0.equals("b");
                    }
                }).oneOrMore()
                .within(Time.seconds(5));
        Pattern<Tuple3<String, Long, String>, ?> pattern1 = Pattern
                .<Tuple3<String, Long, String>>begin("start")
                .where(new RichIterativeCondition<Tuple3<String, Long, String>>() {
                    @Override
                    public boolean filter(Tuple3<String, Long, String> value, Context<Tuple3<String, Long, String>> ctx) throws Exception {
                        return value.f0.equals("a");
                    }
                })
                .followedBy("middle")
                .where(new RichIterativeCondition<Tuple3<String, Long, String>>() {
                    @Override
                    public boolean filter(Tuple3<String, Long, String> value, Context<Tuple3<String, Long, String>> ctx) throws Exception {
                        return value.f0.equals("c");
                    }
                }).or(new RichIterativeCondition<Tuple3<String, Long, String>>() {
                    @Override
                    public boolean filter(Tuple3<String, Long, String> value, Context<Tuple3<String, Long, String>> ctx) throws Exception {
                        return value.f0.equals("d");
                    }
                }).oneOrMore()
                .within(Time.seconds(10));
        Map<String, Pattern<Tuple3<String, Long, String>, ?>> patternMap = new HashMap<String, Pattern<Tuple3<String, Long, String>, ?>>(1);
        patternMap.put("johnPattern", pattern);
        patternMap.put("johnPattern1",pattern1);
        CEP
                .pattern(source, patternMap)
                .select(new RichPatternSelectFunction<Tuple3<String, Long, String>, Map<Tuple2<String, String>, List<Tuple3<String, Long, String>>>>() {
                    @Override
                    public Map<Tuple2<String, String>, List<Tuple3<String, Long, String>>> select(Map<Tuple2<String, String>, List<Tuple3<String, Long, String>>> pattern) throws Exception {
                        return pattern;
                    }
                }).print("john out");

        env.execute("cep");
    }
}

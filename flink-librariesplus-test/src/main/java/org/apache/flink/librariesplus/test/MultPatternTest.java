package org.apache.flink.librariesplus.test;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ceppro.CEP;
import org.apache.flink.ceppro.RichPatternSelectFunction;
import org.apache.flink.ceppro.pattern.Pattern;
import org.apache.flink.ceppro.pattern.conditions.RichIterativeCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * @author johnCai
 * @version 1.0
 * @date 2021/12/15 下午10:43
 */
public class MultPatternTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers", "hadoop01:9092,hadoop02:9092,hadoop03:9092");
        prop.setProperty("group.id", "johnGroup");
        FlinkKafkaConsumer<String> kfc = new FlinkKafkaConsumer<String>("johntest", new SimpleStringSchema(), prop);
//		数据源
//        DataStreamSource<String> stringDataStreamSource = env.addSource(kfc);
//        stringDataStreamSource.print("test");
        SingleOutputStreamOperator<String> source = env.addSource(kfc);
        source.print("output");
        SingleOutputStreamOperator<Tuple3<String, Long, String>> stream = source
                .filter(StringUtils::isNotBlank)
                .map(new MapFunction<String, Tuple3<String, Long, String>>() {
                    @Override
                    public Tuple3<String, Long, String> map(String value) throws Exception {
                        String[] split = value.split(",");
                        return new Tuple3<String, Long, String>(split[0], Long.parseLong(split[1])*1000, split[2]);
                    }
                })
                .assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<Tuple3<String, Long, String>>() {
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
//                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple3<String, Long, String>>(Time.seconds(0L)) {
//                    @Override
//                    public long extractTimestamp(Tuple3<String, Long, String> value) {
//                        return value.f1;
//                    }
//                });

        Pattern<Tuple3<String, Long, String>, ?> pattern = Pattern
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
        patternMap.put("johnPattern1", pattern1);

        CEP
                .pattern(stream, patternMap)
                .registerListener(new MyPatternChangeListener())
                .select(new RichPatternSelectFunction<Tuple3<String, Long, String>, Map<Tuple2<String, String>, List<Tuple3<String, Long, String>>>>() {
                    @Override
                    public Map<Tuple2<String, String>, List<Tuple3<String, Long, String>>> select(Map<Tuple2<String, String>, List<Tuple3<String, Long, String>>> pattern) throws Exception {
                        return pattern;
                    }
                }).print("john out");

        env.execute("cep");
    }
}

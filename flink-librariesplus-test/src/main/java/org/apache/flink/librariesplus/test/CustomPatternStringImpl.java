package org.apache.flink.librariesplus.test;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ceppro.GeneratePatternInterface;
import org.apache.flink.ceppro.pattern.Pattern;
import org.apache.flink.ceppro.pattern.conditions.RichIterativeCondition;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.HashMap;
import java.util.Map;

/**
 * @author johnCai
 * @version 1.0
 * @date 2021/12/19 下午9:46
 */
public class CustomPatternStringImpl implements GeneratePatternInterface {

    public static void main(String[] args) {
        System.out.println("1111111111111111111111111111111111111111");
    }

    public Map<String,Pattern<Tuple3<String, Long, String>,?>> getPatternMap() {
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
        patternMap.put("johnPattern1",pattern);
        return patternMap;
    }
}

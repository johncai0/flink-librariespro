package org.apache.flink.librariesplus.test

import java.util
import java.util.Properties

import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.tuple.{Tuple2, Tuple3}
import org.apache.flink.ceppro.RichPatternSelectFunction
import org.apache.flink.ceppro.pattern.conditions.{IterativeCondition, RichIterativeCondition}
import org.apache.flink.ceppro.scala.CEP
import org.apache.flink.ceppro.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

/**
 * @author johnCai
 * @date 2022/1/1 上午12:36
 * @version 1.0
 */
object MultPatternScalaTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val prop = new Properties
    prop.setProperty("bootstrap.servers", "hadoop01:9092,hadoop02:9092,hadoop03:9092")
    prop.setProperty("group.id", "johnGroup")
    val kfc = new FlinkKafkaConsumer[String]("johntest", new SimpleStringSchema, prop)
    val source = env.addSource(kfc)
    source.print("output")
    val stream = source.filter(StringUtils.isNotBlank(_))
      .map(new MapFunction[String, Tuple3[String, Long, String]]() {
        @throws[Exception]
        override def map(value: String): Tuple3[String, Long, String] = {
          val split = value.split(",")
          new Tuple3[String, Long, String](split(0), split(1).toLong * 1000, split(2))
        }
      }).assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks[Tuple3[String, Long, String]]() {
      private[test] var maxTimsStamp = 0L

      override def checkAndGetNextWatermark(stringLongStringTuple3: Tuple3[String, Long, String], l: Long) = new Watermark(maxTimsStamp)

      override def extractTimestamp(stringLongStringTuple3: Tuple3[String, Long, String], per: Long): Long = {
        val elementTime = stringLongStringTuple3.f1
        if (elementTime > maxTimsStamp) maxTimsStamp = elementTime
        elementTime
      }
    })

    val pattern = Pattern.begin[Tuple3[String, Long, String]]("start").where(new RichIterativeCondition[Tuple3[String, Long, String]]() {
      @throws[Exception]
      override def filter(value: Tuple3[String, Long, String], ctx: IterativeCondition.Context[Tuple3[String, Long, String]]): Boolean = value.f0 == "a"
    }).followedBy("middle").where(new RichIterativeCondition[Tuple3[String, Long, String]]() {
      @throws[Exception]
      override def filter(value: Tuple3[String, Long, String], ctx: IterativeCondition.Context[Tuple3[String, Long, String]]): Boolean = value.f0 == "b"
    }).oneOrMore.within(Time.seconds(5))
    val pattern1 = Pattern.begin[Tuple3[String, Long, String]]("start").where(new RichIterativeCondition[Tuple3[String, Long, String]]() {
      @throws[Exception]
      override def filter(value: Tuple3[String, Long, String], ctx: IterativeCondition.Context[Tuple3[String, Long, String]]): Boolean = value.f0 == "a"
    }).followedBy("middle").where(new RichIterativeCondition[Tuple3[String, Long, String]]() {
      @throws[Exception]
      override def filter(value: Tuple3[String, Long, String], ctx: IterativeCondition.Context[Tuple3[String, Long, String]]): Boolean = value.f0 == "c"
    }).or(new RichIterativeCondition[Tuple3[String, Long, String]]() {
      @throws[Exception]
      override def filter(value: Tuple3[String, Long, String], ctx: IterativeCondition.Context[Tuple3[String, Long, String]]): Boolean = value.f0 == "d"
    }).oneOrMore.within(Time.seconds(10))

    val patternMap = Map("johnPattern" -> pattern, "johnPattern1" -> pattern1)

    CEP.pattern(stream, patternMap)
      .select(new RichPatternSelectFunction[Tuple3[String, Long, String], util.Map[Tuple2[String, String], util.List[Tuple3[String, Long, String]]]]() {
        @throws[Exception]
        override def select(pattern: util.Map[Tuple2[String, String], util.List[Tuple3[String, Long, String]]]): util.Map[Tuple2[String, String], util.List[Tuple3[String, Long, String]]] = pattern
      }).print("john out")


    env.execute("cep")
  }
}

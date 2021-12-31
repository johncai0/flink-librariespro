/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.ceppro.scala

import java.util

import org.apache.flink.ceppro.pattern.{Pattern => JPattern}
import org.apache.flink.ceppro.scala.pattern.Pattern
import org.apache.flink.ceppro.{EventComparator, CEP => JCEP}
import org.apache.flink.streaming.api.scala.DataStream

import scala.collection.JavaConverters.mapAsScalaMapConverter
import scala.collection.mutable

/**
  * Utility method to transform a [[DataStream]] into a [[PatternStream]] to do CEP.
  */

object CEP {
  /**
    * Transforms a [[DataStream]] into a [[PatternStream]] in the Scala API.
    * See [[org.apache.flink.ceppro.CEP}]]for a more detailed description how the underlying
    * Java API works.
    *
    * @param input   DataStream containing the input events
    * @param patternMap Pattern specification which shall be detected
    * @tparam T Type of the input events
    * @return Resulting pattern stream
    */
//  def pattern[T](input: DataStream[T], pattern: Pattern[T, _ <: T]): PatternStream[T] = {
//    wrapPatternStream(JCEP.pattern(input.javaStream, pattern.wrappedPattern))
//  }

  def pattern[T](input: DataStream[T], patternMap: mutable.Map[String,Pattern[T, _ <: T]]): PatternStream[T] = {
    wrapPatternStream(JCEP.pattern(input.javaStream, convPattern(patternMap)))
  }

  def pattern[T](input: DataStream[T], patternMap: util.Map[String,Pattern[T, _ <: T]]): PatternStream[T] = {
    val javaPatternMap = new util.HashMap[String,JPattern[T,_ <: T]]()
    patternMap.asScala.foreach(l => {
      javaPatternMap.put(l._1,l._2.wrappedPattern)
    })
    wrapPatternStream(JCEP.pattern(input.javaStream, convPattern(patternMap)))
  }

  /**
    * Transforms a [[DataStream]] into a [[PatternStream]] in the Scala API.
    * See [[org.apache.flink.ceppro.CEP}]]for a more detailed description how the underlying
    * Java API works.
    *
    * @param input      DataStream containing the input events
    * @param patternMap    Pattern specification which shall be detected
    * @param comparator Comparator to sort events with equal timestamps
    * @tparam T Type of the input events
    * @return Resulting pattern stream
    */
//  def pattern[T](
//    input: DataStream[T],
//    pattern: Pattern[T, _ <: T],
//    comparator: EventComparator[T]): PatternStream[T] = {
//    wrapPatternStream(JCEP.pattern(input.javaStream, pattern.wrappedPattern, comparator))
//  }

  def pattern[T](input: DataStream[T], patternMap: mutable.Map[String,Pattern[T, _ <: T]],comparator: EventComparator[T]): PatternStream[T] = {
    wrapPatternStream(JCEP.pattern(input.javaStream, convPattern(patternMap),comparator))
  }

  def pattern[T](input: DataStream[T], patternMap: util.Map[String,Pattern[T, _ <: T]],comparator: EventComparator[T]): PatternStream[T] = {
    wrapPatternStream(JCEP.pattern(input.javaStream, convPattern(patternMap),comparator))
  }

  private def convPattern[T](patternMap: mutable.Map[String,Pattern[T, _ <: T]]): util.HashMap[String,JPattern[T,_ <: T]] = {
    val javaPatternMap = new util.HashMap[String,JPattern[T,_ <: T]](patternMap.size)
    patternMap.foreach(l => {
      javaPatternMap.put(l._1,l._2.wrappedPattern)
    })
    javaPatternMap
  }
  private def convPattern[T](patternMap: util.Map[String,Pattern[T, _ <: T]]): util.HashMap[String,JPattern[T,_ <: T]] = {
    val javaPatternMap = new util.HashMap[String,JPattern[T,_ <: T]](patternMap.size())
    patternMap.asScala.foreach(l => {
      javaPatternMap.put(l._1,l._2.wrappedPattern)
    })
    javaPatternMap
  }
}


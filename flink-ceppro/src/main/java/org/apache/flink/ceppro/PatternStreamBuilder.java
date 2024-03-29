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

package org.apache.flink.ceppro;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.functions.NullByteKeySelector;
import org.apache.flink.ceppro.functions.PatternProcessFunction;
import org.apache.flink.ceppro.functions.TimedOutPartialMatchHandler;
import org.apache.flink.ceppro.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.ceppro.nfa.compiler.NFACompiler;
import org.apache.flink.ceppro.operator.CepOperator;
import org.apache.flink.ceppro.pattern.Pattern;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Utility method for creating {@link PatternStream}.
 */
@Internal
final class PatternStreamBuilder<IN> {

	private final DataStream<IN> inputStream;

	private final Map<String,Pattern<IN, ?>> patterns;

	private final EventComparator<IN> comparator;

	/**
	 * Side output {@code OutputTag} for late data.
	 * If no tag is set late data will be simply dropped.
	 */
	private final OutputTag<IN> lateDataOutputTag;

	private PatternStreamBuilder(
			final DataStream<IN> inputStream,
			final Map<String,Pattern<IN, ?>> patterns,
			@Nullable final EventComparator<IN> comparator,
			@Nullable final OutputTag<IN> lateDataOutputTag) {

		this.inputStream = checkNotNull(inputStream);
		this.patterns = checkNotNull(patterns);
		this.comparator = comparator;
		this.lateDataOutputTag = lateDataOutputTag;
	}

	TypeInformation<IN> getInputType() {
		return inputStream.getType();
	}

	/**
	 * Invokes the {@link org.apache.flink.api.java.ClosureCleaner}
	 * on the given function if closure cleaning is enabled in the {@link ExecutionConfig}.
	 *
	 * @return The cleaned Function
	 */
	<F> F clean(F f) {
		return inputStream.getExecutionEnvironment().clean(f);
	}

	PatternStreamBuilder<IN> withComparator(final EventComparator<IN> comparator) {
		return new PatternStreamBuilder<>(inputStream, patterns, checkNotNull(comparator), lateDataOutputTag);
	}

	PatternStreamBuilder<IN> withLateDataOutputTag(final OutputTag<IN> lateDataOutputTag) {
		return new PatternStreamBuilder<>(inputStream, patterns, comparator, checkNotNull(lateDataOutputTag));
	}

	/**
	 * Creates a data stream containing results of {@link PatternProcessFunction} to fully matching event patterns.
	 *
	 * @param processFunction function to be applied to matching event sequences
	 * @param outTypeInfo output TypeInformation of
	 *        {@link PatternProcessFunction#processMatch(Map, PatternProcessFunction.Context, Collector)}
	 * @param <OUT> type of output events
	 * @return Data stream containing fully matched event sequence with applied {@link PatternProcessFunction}
	 */
	<OUT, K> SingleOutputStreamOperator<OUT> build(
			final TypeInformation<OUT> outTypeInfo,
			final PatternProcessFunction<IN, OUT> processFunction) {
		// PatternProcessFunction 经过封装的selectFuncation
		checkNotNull(outTypeInfo);
		checkNotNull(processFunction);

		final TypeSerializer<IN> inputSerializer = inputStream.getType().createSerializer(inputStream.getExecutionConfig());
		final boolean isProcessingTime = inputStream.getExecutionEnvironment().getStreamTimeCharacteristic() == TimeCharacteristic.ProcessingTime;

		final boolean timeoutHandling = processFunction instanceof TimedOutPartialMatchHandler;
		final Map<String,NFACompiler.NFAFactory<IN>> nfaFactoryMap = NFACompiler.compileFactory(patterns, timeoutHandling);
		final Map<String, AfterMatchSkipStrategy> afterMatchSkipStrategyMap = new HashMap<>(patterns.size());
		patterns.forEach((k,v) -> afterMatchSkipStrategyMap.put(k,v.getAfterMatchSkipStrategy()));

		final CepOperator<IN, K, OUT> operator = new CepOperator<IN, K, OUT>(
			inputSerializer,
			isProcessingTime,
			nfaFactoryMap,
			comparator,
			afterMatchSkipStrategyMap,
			processFunction,
			lateDataOutputTag);

		final SingleOutputStreamOperator<OUT> patternStream;
		if (inputStream instanceof KeyedStream) {
			KeyedStream<IN, K> keyedStream = (KeyedStream<IN, K>) inputStream;

			patternStream = keyedStream.transform(
				"CepOperator",
				outTypeInfo,
				operator);
		} else {
			KeySelector<IN, Byte> keySelector = new NullByteKeySelector<>();

			patternStream = inputStream.keyBy(keySelector).transform(
				"GlobalCepOperator",
				outTypeInfo,
				operator
			).forceNonParallel();
		}

		return patternStream;
	}

	// ---------------------------------------- factory-like methods ---------------------------------------- //

//	static <IN> PatternStreamBuilder<IN> forStreamAndPattern(final DataStream<IN> inputStream, final Pattern<IN, ?> pattern) {
//		return new PatternStreamBuilder<>(inputStream, pattern, null, null);
//	}
	static <IN> PatternStreamBuilder<IN> forStreamAndPattern(final DataStream<IN> inputStream, final Map<String,Pattern<IN, ?>> patterns) {
		return new PatternStreamBuilder<>(inputStream, patterns, null, null);
	}
}

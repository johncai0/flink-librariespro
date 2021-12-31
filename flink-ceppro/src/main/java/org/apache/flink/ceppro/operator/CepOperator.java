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

package org.apache.flink.ceppro.operator;

import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ceppro.EventComparator;
import org.apache.flink.ceppro.functions.PatternProcessFunction;
import org.apache.flink.ceppro.functions.TimedOutPartialMatchHandler;
import org.apache.flink.ceppro.nfa.NFA;
import org.apache.flink.ceppro.nfa.NFA.MigratedNFA;
import org.apache.flink.ceppro.nfa.NFAState;
import org.apache.flink.ceppro.nfa.NFAStateSerializer;
import org.apache.flink.ceppro.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.ceppro.nfa.compiler.NFACompiler;
import org.apache.flink.ceppro.nfa.sharedbuffer.SharedBuffer;
import org.apache.flink.ceppro.nfa.sharedbuffer.SharedBufferAccessor;
import org.apache.flink.ceppro.pattern.Pattern;
import org.apache.flink.ceppro.time.TimerService;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.state.KeyedStateFunction;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.*;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.*;
import java.util.stream.Stream;

/**
 * CEP pattern operator for a keyed input stream. For each key, the operator creates
 * a {@link NFA} and a priority queue to buffer out of order elements. Both data structures are
 * stored using the managed keyed state.
 *
 * @param <IN>  Type of the input elements
 * @param <KEY> Type of the key on which the input stream is keyed
 * @param <OUT> Type of the output elements
 */
@Internal
public class CepOperator<IN, KEY, OUT>
        extends AbstractUdfStreamOperator<OUT, PatternProcessFunction<IN, OUT>>
        implements OneInputStreamOperator<IN, OUT>, Triggerable<KEY, VoidNamespace> {
    private static final Logger LOG = LoggerFactory.getLogger(CepOperator.class);
    private static final long serialVersionUID = -4166778210774160757L;

    private static final String LATE_ELEMENTS_DROPPED_METRIC_NAME = "numLateRecordsDropped";

    private final boolean isProcessingTime;

    private final TypeSerializer<IN> inputSerializer;

    ///////////////			State			//////////////

    private static final String NFA_STATE_NAME = "nfaStateName";
    private static final String EVENT_QUEUE_STATE_NAME = "eventQueuesStateName";

    private final Map<String, NFACompiler.NFAFactory<IN>> nfaFactoryMap;

    private transient MapState<String, NFAState> computationStateMap;
    private transient MapState<Long, List<IN>> elementQueueState;
    private transient SharedBuffer<IN> partialMatches;

    private transient InternalTimerService<VoidNamespace> timerService;

    private transient Map<String, NFA<IN>> nfaMap;

    private transient long lastUpdateTimestamp;

    /**
     * The last seen watermark. This will be used to
     * decide if an incoming element is late or not.
     */
    private long lastWatermark;

    /**
     * Comparator for secondary sorting. Primary sorting is always done on time.
     */
    private final EventComparator<IN> comparator;

    /**
     * {@link OutputTag} to use for late arriving events. Elements with timestamp smaller than
     * the current watermark will be emitted to this.
     */
    private final OutputTag<IN> lateDataOutputTag;

    /**
     * Strategy which element to skip after a match was found.
     */
    private Map<String, AfterMatchSkipStrategy> afterMatchSkipStrategyMap;

    /**
     * Context passed to user function.
     */
    private transient ContextFunctionImpl context;

    /**
     * Main output collector, that sets a proper timestamp to the StreamRecord.
     */
    private transient TimestampedCollector<OUT> collector;

    /**
     * Wrapped RuntimeContext that limits the underlying context features.
     */
    private transient CepRuntimeContext cepRuntimeContext;

    /**
     * Thin context passed to NFA that gives access to time related characteristics.
     */
    private transient TimerService cepTimerService;

    // ------------------------------------------------------------------------
    // Metrics
    // ------------------------------------------------------------------------

    private transient Counter numLateRecordsDropped;

    public CepOperator(
            final TypeSerializer<IN> inputSerializer,
            final boolean isProcessingTime,
            final Map<String, NFACompiler.NFAFactory<IN>> nfaFactoryMap,
            @Nullable final EventComparator<IN> comparator,
            @Nullable final Map<String, AfterMatchSkipStrategy> afterMatchSkipStrategyMap,
            final PatternProcessFunction<IN, OUT> function,
            @Nullable final OutputTag<IN> lateDataOutputTag) {
        super(function);

        this.inputSerializer = Preconditions.checkNotNull(inputSerializer);
        this.nfaFactoryMap = Preconditions.checkNotNull(nfaFactoryMap);

        this.isProcessingTime = isProcessingTime;
        this.comparator = comparator;
        this.lateDataOutputTag = lateDataOutputTag;
        this.afterMatchSkipStrategyMap = new HashMap<>(afterMatchSkipStrategyMap.size());
        afterMatchSkipStrategyMap.forEach((k, v) -> {
            if (v == null) {
                this.afterMatchSkipStrategyMap.put(k, AfterMatchSkipStrategy.noSkip());
            } else {
                this.afterMatchSkipStrategyMap.put(k, v);
            }
        });
    }

    @Override
    public void setup(StreamTask<?, ?> containingTask, StreamConfig config, Output<StreamRecord<OUT>> output) {
        super.setup(containingTask, config, output);
        this.cepRuntimeContext = new CepRuntimeContext(getRuntimeContext());
        FunctionUtils.setFunctionRuntimeContext(getUserFunction(), this.cepRuntimeContext);
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);

        // initializeState through the provided context
        computationStateMap = context.getKeyedStateStore().getMapState(
                new MapStateDescriptor<>(
                        NFA_STATE_NAME,
                        StringSerializer.INSTANCE,
                        new NFAStateSerializer()));

        partialMatches = new SharedBuffer<>(context.getKeyedStateStore(), inputSerializer);

        elementQueueState = context.getKeyedStateStore().getMapState(
                new MapStateDescriptor<>(
                        EVENT_QUEUE_STATE_NAME,
                        LongSerializer.INSTANCE,
                        new ListSerializer<>(inputSerializer)));

        migrateOldState();
    }

    private void migrateOldState() throws Exception {
        getKeyedStateBackend().applyToAllKeys(
                VoidNamespace.INSTANCE,
                VoidNamespaceSerializer.INSTANCE,
                new MapStateDescriptor<>(
                        "nfaOperatorStateName",
                        StringSerializer.INSTANCE,
                        new NFA.NFASerializer<>(inputSerializer)
                ),
                new KeyedStateFunction<Object, MapState<String, MigratedNFA<IN>>>() {
                    @Override
                    public void process(Object key, MapState<String, MigratedNFA<IN>> state) throws Exception {
                        for (String statKey : state.keys()) {
                            MigratedNFA<IN> oldState = state.get(statKey);
                            computationStateMap.put(statKey, new NFAState(oldState.getComputationStates()));
                            org.apache.flink.ceppro.nfa.SharedBuffer<IN> sharedBuffer = oldState.getSharedBuffer();
                            partialMatches.init(sharedBuffer.getEventsBuffer(), sharedBuffer.getPages());
                        }

                        state.clear();
                    }
                }
        );
    }

    @Override
    public void open() throws Exception {
        super.open();
        timerService = getInternalTimerService(
                "watermark-callbacks",
                VoidNamespaceSerializer.INSTANCE,
                this);
        nfaMap = new HashMap<>(nfaFactoryMap.size());
        for (Map.Entry<String, NFACompiler.NFAFactory<IN>> entry : nfaFactoryMap.entrySet()) {
            NFA<IN> nfa = entry.getValue().createNFA();
            nfa.open(cepRuntimeContext, new Configuration());
            nfaMap.put(entry.getKey(), nfa);
        }

        context = new ContextFunctionImpl();
        collector = new TimestampedCollector<>(output);
        cepTimerService = new TimerServiceImpl();

        // metrics
        this.numLateRecordsDropped = metrics.counter(LATE_ELEMENTS_DROPPED_METRIC_NAME);
        this.lastUpdateTimestamp = System.currentTimeMillis();
    }

    public void checkAndUpdatePattern(PatternProcessFunction<IN, OUT> userFunction) throws Exception {
        if (userFunction.needDynamicPattern() &&
                (System.currentTimeMillis() - this.lastUpdateTimestamp) >= userFunction.getUpdateInterval() &&
                userFunction.needChange()) {
            Map<String, Pattern<IN, ?>> newPatterns = userFunction.getNewPattern();
            if (newPatterns != null && newPatterns.size() > 0) {
                final boolean timeoutHandling = userFunction instanceof TimedOutPartialMatchHandler;
                final Map<String,NFACompiler.NFAFactory<IN>> nfaFactoryMap = NFACompiler.compileFactory(newPatterns, timeoutHandling);

                //更新NFA
                HashMap<String, NFA<IN>> newNfaMap = new HashMap<>(newPatterns.size()+nfaMap.size());
                //既然输入了新的pattern和对应的key，并且这个key原来已经存在了，所以需要更新，更新的话就需要清空状态
                //所以需要将这个key记录到changedKey中
                Set<String> changedKey = new HashSet<>(newPatterns.size());
                for (Map.Entry<String, NFACompiler.NFAFactory<IN>> entry : nfaFactoryMap.entrySet()) {
                    NFA<IN> nfa = entry.getValue().createNFA();
                    nfa.open(cepRuntimeContext, new Configuration());
                    if (this.nfaMap.containsKey(entry.getKey())) {
                        LOG.info("update pattern {}",entry.getKey());
                        changedKey.add(entry.getKey());
                    }
                    newNfaMap.put(entry.getKey(), nfa);
                }
                Set<String> needDel = userFunction.getDelPatternKey();
                if (needDel != null && needDel.size()>0) {
                    for (String nd : needDel) {
                        nfaMap.remove(nd);
                    }
                }
                newNfaMap.putAll(nfaMap);
                this.nfaMap = newNfaMap;

                //知道那些pattern发生了变化（判断逻辑还有些问题，除了图外，还有条件）
                //那些pattern需要删除，那么需要进行2步骤
                if (needDel != null && needDel.size() > 0) changedKey.addAll(needDel);
                if (changedKey.size()>0) cleanBeforeMatch(changedKey);

                //更新跳过策略 滞后处理
                updateAfterMatchSkipStrategyMap(changedKey,newPatterns);

                //清除SB
                partialMatches.clean(needDel);
            }
        }
    }

    private void updateAfterMatchSkipStrategyMap(Set<String> changedKey,Map<String, Pattern<IN, ?>> newPatterns) {
        if (CollectionUtils.isNotEmpty(changedKey)) {
            changedKey.forEach(k -> {
                afterMatchSkipStrategyMap.remove(k);
            });
        }
        newPatterns.forEach((k,v) -> {
            if (v.getAfterMatchSkipStrategy() != null) {
                afterMatchSkipStrategyMap.put(k, v.getAfterMatchSkipStrategy());
            } else {
                afterMatchSkipStrategyMap.put(k,AfterMatchSkipStrategy.noSkip());
            }
        });
    }

    private void cleanBeforeMatch(Set<String> keys) throws Exception {
//		将原来的为匹配完全的状态清理
        for (String key:keys) {
            computationStateMap.remove(key);
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        for (NFA<IN> nfa : nfaMap.values())
            if (nfa != null) {
                nfa.close();
            }
    }

    private void update() throws Exception {
        PatternProcessFunction<IN, OUT> userFunction = getUserFunction();
        if (userFunction.getUpdateInterval() > 0 &&
                System.currentTimeMillis()-this.timerService.currentProcessingTime() > userFunction.getUpdateInterval()) {
            checkAndUpdatePattern(userFunction);
        }
    }

    @Override
    public void processElement(StreamRecord<IN> element) throws Exception {
        if (isProcessingTime) {
            update();
            if (comparator == null) {
                Map<String,NFAState> stateMap= convNFAStateToMap();
                long timestamp = getProcessingTimeService().getCurrentProcessingTime();
                // there can be no out of order elements in processing time
                advanceTime(stateMap, timestamp);
                processEvent(stateMap, element.getValue(), timestamp);
                updateNFA(stateMap);
            } else {
                //此处只需要将数据buffer起来，不用处理，所以不需要循环多个pattern的恩题 --明天继续
                long currentTime = timerService.currentProcessingTime();
                bufferEvent(element.getValue(), currentTime);

                // register a timer for the next millisecond to sort and emit buffered data
                timerService.registerProcessingTimeTimer(VoidNamespace.INSTANCE, currentTime + 1);
            }

        } else {

            long timestamp = element.getTimestamp();
            IN value = element.getValue();

            // In event-time processing we assume correctness of the watermark.
            // Events with timestamp smaller than or equal with the last seen watermark are considered late.
            // Late events are put in a dedicated side output, if the user has specified one.

            if (timestamp > lastWatermark) {

                // we have an event with a valid timestamp, so
                // we buffer it until we receive the proper watermark.

                saveRegisterWatermarkTimer();

                bufferEvent(value, timestamp);

            } else if (lateDataOutputTag != null) {
                output.collect(lateDataOutputTag, element);
            } else {
                numLateRecordsDropped.inc();
            }
        }
    }

    /**
     * Registers a timer for {@code current watermark + 1}, this means that we get triggered
     * whenever the watermark advances, which is what we want for working off the queue of
     * buffered elements.
     */
    private void saveRegisterWatermarkTimer() {
        long currentWatermark = timerService.currentWatermark();
        // protect against overflow
        if (currentWatermark + 1 > currentWatermark) {
            timerService.registerEventTimeTimer(VoidNamespace.INSTANCE, currentWatermark + 1);
        }
    }

    private void bufferEvent(IN event, long currentTime) throws Exception {
        List<IN> elementsForTimestamp = elementQueueState.get(currentTime);
        if (elementsForTimestamp == null) {
            elementsForTimestamp = new ArrayList<>();
        }

        elementsForTimestamp.add(event);
        elementQueueState.put(currentTime, elementsForTimestamp);
    }

    @Override
    public void onEventTime(InternalTimer<KEY, VoidNamespace> timer) throws Exception {
        PatternProcessFunction<IN, OUT> userFunction = getUserFunction();
        if (userFunction.getUpdateInterval() > 0 &&
                System.currentTimeMillis()-this.timerService.currentWatermark() > userFunction.getUpdateInterval()) {
            checkAndUpdatePattern(userFunction);
        }
        // 1) get the queue of pending elements for the key and the corresponding NFA,
        // 2) process the pending elements in event time order and custom comparator if exists
        //		by feeding them in the NFA
        // 3) advance the time to the current watermark, so that expired patterns are discarded.
        // 4) update the stored state for the key, by only storing the new NFA and MapState iff they
        //		have state to be used later.
        // 5) update the last seen watermark.

        // STEP 1
        PriorityQueue<Long> sortedTimestamps = getSortedTimestamps();
        Map<String,NFAState> stateMap = convNFAStateToMap();

        // STEP 2
        while (!sortedTimestamps.isEmpty() && sortedTimestamps.peek() <= timerService.currentWatermark()) {
            long timestamp = sortedTimestamps.poll();
            advanceTime(stateMap, timestamp);
            try (Stream<IN> elements = sort(elementQueueState.get(timestamp))) {
                elements.forEachOrdered(
                        event -> {
                            try {
                                processEvent(stateMap, event, timestamp);
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        }
                );
            }
            elementQueueState.remove(timestamp);
        }

        // STEP 3
        advanceTime(stateMap, timerService.currentWatermark());


        // STEP 4
        updateNFA(stateMap);

        if (!sortedTimestamps.isEmpty() || !partialMatches.isEmpty()) {
            saveRegisterWatermarkTimer();
        }

        // STEP 5
        updateLastSeenWatermark(timerService.currentWatermark());
        //partialMatches.printShardBufferInfo();
    }

    private Map<String,NFAState> convNFAStateToMap() throws Exception {
        Map<String,NFAState> stateMap = new HashMap<>();
        for (String key : nfaMap.keySet()) {
            NFAState nfaState = getNFAState(key);
            stateMap.put(key,nfaState);
        }
        return stateMap;
    }

    @Override
    public void onProcessingTime(InternalTimer<KEY, VoidNamespace> timer) throws Exception {
        update();
        // 1) get the queue of pending elements for the key and the corresponding NFA,
        // 2) process the pending elements in process time order and custom comparator if exists
        //		by feeding them in the NFA
        // 3) update the stored state for the key, by only storing the new NFA and MapState iff they
        //		have state to be used later.

        // STEP 1
        PriorityQueue<Long> sortedTimestamps = getSortedTimestamps();
        Map<String,NFAState> stateMap = convNFAStateToMap();


        // STEP 2
        while (!sortedTimestamps.isEmpty()) {
            long timestamp = sortedTimestamps.poll();
            advanceTime(stateMap, timestamp);
            try (Stream<IN> elements = sort(elementQueueState.get(timestamp))) {
                elements.forEachOrdered(
                        event -> {
                            try {
                                processEvent(stateMap, event, timestamp);
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        }
                );
            }
            elementQueueState.remove(timestamp);
        }

        // STEP 3
        updateNFA(stateMap);
    }

    private Stream<IN> sort(Collection<IN> elements) {
        Stream<IN> stream = elements.stream();
        return (comparator == null) ? stream : stream.sorted(comparator);
    }

    private void updateLastSeenWatermark(long timestamp) {
        this.lastWatermark = timestamp;
    }

    private NFAState getNFAState(String patternKey) throws Exception {
        NFAState nfaState = computationStateMap.get(patternKey);
        if (nfaState == null) {
            nfaState = nfaMap.get(patternKey).createInitialNFAState();
        }
        return nfaState;
    }

    private void updateNFA(Map<String,NFAState> stateMap) throws Exception {
        for (String key : stateMap.keySet()) {
            if (stateMap.get(key).isStateChanged()) {
                stateMap.get(key).resetStateChanged();
                computationStateMap.put(key,stateMap.get(key));
            }
        }
    }

    private PriorityQueue<Long> getSortedTimestamps() throws Exception {
        PriorityQueue<Long> sortedTimestamps = new PriorityQueue<>();
        for (Long timestamp : elementQueueState.keys()) {
            sortedTimestamps.offer(timestamp);
        }
        return sortedTimestamps;
    }

    /**
     * Process the given event by giving it to the NFA and outputting the produced set of matched
     * event sequences.
     *
     * @param nfaStateMap  Our NFAState object
     * @param event     The current event to be processed
     * @param timestamp The timestamp of the event
     */
    private void processEvent(Map<String,NFAState> nfaStateMap, IN event, long timestamp) throws Exception {
        try (SharedBufferAccessor<IN> sharedBufferAccessor = partialMatches.getAccessor()) {
            for (String key:nfaStateMap.keySet()) {
                Collection<Map<Tuple2<String, String>, List<IN>>> patterns =
                        nfaMap.get(key).process(
                                sharedBufferAccessor,
                                nfaStateMap.get(key),
                                event,
                                timestamp,
                                afterMatchSkipStrategyMap.get(key),
                                cepTimerService);
                processMatchedSequences(patterns, timestamp);
            }
        }
    }

    /**
     * Advances the time for the given NFA to the given timestamp. This means that no more events with timestamp
     * <b>lower</b> than the given timestamp should be passed to the nfa, This can lead to pruning and timeouts.
     */
    private void advanceTime(Map<String,NFAState> nfaStateMap, long timestamp) throws Exception {
        try (SharedBufferAccessor<IN> sharedBufferAccessor = partialMatches.getAccessor()) {
            for (String key:nfaStateMap.keySet()) {
                Collection<Tuple2<Map<Tuple2<String,String>, List<IN>>, Long>> timedOut =
                        nfaMap.get(key).advanceTime(sharedBufferAccessor, nfaStateMap.get(key), timestamp);
                if (!timedOut.isEmpty()) {
                    processTimedOutSequences(timedOut);
                }
            }
        }
    }

    private void processMatchedSequences(Iterable<Map<Tuple2<String,String>, List<IN>>> matchingSequences, long timestamp) throws Exception {
        PatternProcessFunction<IN, OUT> function = getUserFunction();
        setTimestamp(timestamp);
        for (Map<Tuple2<String,String>, List<IN>> matchingSequence : matchingSequences) {
            function.processMatch(matchingSequence, context, collector);
        }
    }

    private void processTimedOutSequences(Collection<Tuple2<Map<Tuple2<String,String>, List<IN>>, Long>> timedOutSequences) throws Exception {
        PatternProcessFunction<IN, OUT> function = getUserFunction();
        if (function instanceof TimedOutPartialMatchHandler) {

            @SuppressWarnings("unchecked")
            TimedOutPartialMatchHandler<IN> timeoutHandler = (TimedOutPartialMatchHandler<IN>) function;

            for (Tuple2<Map<Tuple2<String,String>, List<IN>>, Long> matchingSequence : timedOutSequences) {
                setTimestamp(matchingSequence.f1);
                timeoutHandler.processTimedOutMatch(matchingSequence.f0, context);
            }
        }
    }

    private void setTimestamp(long timestamp) {
        if (!isProcessingTime) {
            collector.setAbsoluteTimestamp(timestamp);
        }

        context.setTimestamp(timestamp);
    }

    /**
     * Gives {@link NFA} access to {@link InternalTimerService} and tells if {@link CepOperator} works in
     * processing time. Should be instantiated once per operator.
     */
    private class TimerServiceImpl implements TimerService {

        @Override
        public long currentProcessingTime() {
            return timerService.currentProcessingTime();
        }

    }

    /**
     * Implementation of {@link PatternProcessFunction.Context}. Design to be instantiated once per operator.
     * It serves three methods:
     * <ul>
     *     <li>gives access to currentProcessingTime through {@link InternalTimerService}</li>
     *     <li>gives access to timestamp of current record (or null if Processing time)</li>
     *     <li>enables side outputs with proper timestamp of StreamRecord handling based on either Processing or
     *         Event time</li>
     * </ul>
     */
    private class ContextFunctionImpl implements PatternProcessFunction.Context {

        private Long timestamp;

        @Override
        public <X> void output(final OutputTag<X> outputTag, final X value) {
            final StreamRecord<X> record;
            if (isProcessingTime) {
                record = new StreamRecord<>(value);
            } else {
                record = new StreamRecord<>(value, timestamp());
            }
            output.collect(outputTag, record);
        }

        void setTimestamp(long timestamp) {
            this.timestamp = timestamp;
        }

        @Override
        public long timestamp() {
            return timestamp;
        }

        @Override
        public long currentProcessingTime() {
            return timerService.currentProcessingTime();
        }
    }

    //////////////////////			Testing Methods			//////////////////////

    @VisibleForTesting
    boolean hasNonEmptySharedBuffer(KEY key) throws Exception {
        setCurrentKey(key);
        return !partialMatches.isEmpty();
    }

    @VisibleForTesting
    boolean hasNonEmptyPQ(KEY key) throws Exception {
        setCurrentKey(key);
        return !elementQueueState.isEmpty();
    }

    @VisibleForTesting
    int getPQSize(KEY key) throws Exception {
        setCurrentKey(key);
        int counter = 0;
        for (List<IN> elements : elementQueueState.values()) {
            counter += elements.size();
        }
        return counter;
    }

    @VisibleForTesting
    long getLateRecordsNumber() {
        return numLateRecordsDropped.getCount();
    }
}

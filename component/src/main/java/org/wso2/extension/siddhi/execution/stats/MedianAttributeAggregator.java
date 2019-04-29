/*
* Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package org.wso2.extension.siddhi.execution.stats;

import io.siddhi.annotation.Example;
import io.siddhi.annotation.Extension;
import io.siddhi.annotation.Parameter;
import io.siddhi.annotation.ReturnAttribute;
import io.siddhi.annotation.util.DataType;
import io.siddhi.core.config.SiddhiQueryContext;
import io.siddhi.core.exception.OperationNotSupportedException;
import io.siddhi.core.executor.ExpressionExecutor;
import io.siddhi.core.query.processor.ProcessingMode;
import io.siddhi.core.query.selector.attribute.aggregator.AttributeAggregatorExecutor;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateFactory;
import io.siddhi.query.api.definition.Attribute;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * This extension returns the median of aggregated events.
 */
@Extension(
        name = "median",
        namespace = "stats",
        description = "This extension returns the median of aggregated events." +
                "\n " +
                "As the calculation of the median is performed with the arrival and expiry of each event, " +
                "it is not recommended to use this extension for large window sizes.",
        parameters = {
                @Parameter(name = "data",
                        description = "The value that needs to be aggregated in order to obtain its median.",
                        type = {DataType.INT, DataType.LONG, DataType.DOUBLE, DataType.FLOAT})
        },
        returnAttributes = @ReturnAttribute(
                description = "Returns the median as a 'double' value for all data " +
                        "types, i.e., for 'int', 'long', 'double' and 'float'.",
                type = {DataType.DOUBLE}),
        examples = @Example(
                syntax = "from InputStream#window.length(5)" +
                        "\nselect stats:median(value) as medianOfValues" +
                        "\ninsert into OutputStream;",
                description = "This returns the median of the aggregated values as a 'double' " +
                        "value with the arrival and expiry of each event within the sliding window length of five."
        )
)
public class MedianAttributeAggregator extends AttributeAggregatorExecutor<MedianAttributeAggregator.ExtensionState> {

    private MedianAggregator medianAggregator;

    @Override
    protected StateFactory<ExtensionState> init(ExpressionExecutor[] expressionExecutors,
                                                ProcessingMode processingMode,
                                                boolean b,
                                                ConfigReader configReader,
                                                SiddhiQueryContext siddhiQueryContext) {
        if (expressionExecutors.length != 1) {
            throw new OperationNotSupportedException("Median aggregator has to have exactly 1 " +
                    "parameter, currently " + expressionExecutors.length + " parameters are " +
                    "provided");
        }

        Attribute.Type type = expressionExecutors[0].getReturnType();

        switch (type) {
            case DOUBLE:
                medianAggregator = new MedianAggregatorDouble();
                break;
            case INT:
                medianAggregator = new MedianAggregatorInteger();
                break;
            case FLOAT:
                medianAggregator = new MedianAggregatorFloat();
                break;
            case LONG:
                medianAggregator = new MedianAggregatorLong();
                break;
            default:
                throw new OperationNotSupportedException("Median not supported for " + type);
        }
        return () -> new ExtensionState();
    }

    public Attribute.Type getReturnType() {
        return Attribute.Type.DOUBLE;
    }

    @Override
    public Object processAdd(Object o, ExtensionState extensionState) {
        return medianAggregator.processAdd(o);
    }

    @Override
    public Object processAdd(Object[] objects, ExtensionState extensionState) {
        return new IllegalStateException("Median cannot process data array, but found " +
                Arrays.deepToString(objects));
    }

    @Override
    public Object processRemove(Object o, ExtensionState extensionState) {
        return medianAggregator.processRemove(o);
    }

    @Override
    public Object processRemove(Object[] objects, ExtensionState extensionState) {
        return new IllegalStateException("Median cannot process data array, but found " +
                Arrays.deepToString(objects));
    }

    @Override
    public Object reset(ExtensionState extensionState) {
        return medianAggregator.reset();
    }

    class ExtensionState extends State {

        private static final String KEY_COUNT = "Count";
        private static final String KEY_MEDIAN = "Median";
        private final Map<String, Object> state = new HashMap<>();

        @Override
        public boolean canDestroy() {
            return medianAggregator.getValueCount() == 0
                    && medianAggregator.getCount() == 0
                    && medianAggregator.getMedian() == 0.0;
        }

        @Override
        public Map<String, Object> snapshot() {
            state.put(KEY_MEDIAN, medianAggregator.getMedian());
            state.put(KEY_COUNT, medianAggregator.getCount());
            return state;
        }

        @Override
        public void restore(Map<String, Object> state) {
            medianAggregator.setMedian((Double) state.get(KEY_MEDIAN));
            medianAggregator.setCount((Integer) state.get(KEY_COUNT));
        }
    }

    private static class MedianAggregatorDouble extends MedianAggregator<Double> {

        @Override
        protected double add(Double a, Double b) {
            return a + b;
        }
    }

    private static class MedianAggregatorInteger extends MedianAggregator<Integer> {

        @Override
        protected double add(Integer a, Integer b) {
            return a + b;
        }
    }

    private static class MedianAggregatorFloat extends MedianAggregator<Float> {

        @Override
        protected double add(Float a, Float b) {
            return a + b;
        }
    }

    private static class MedianAggregatorLong extends MedianAggregator<Long> {

        @Override
        protected double add(Long a, Long b) {
            return a + b;
        }
    }
}

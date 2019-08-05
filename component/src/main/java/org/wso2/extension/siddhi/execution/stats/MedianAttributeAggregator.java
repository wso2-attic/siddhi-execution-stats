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

/*
* Copyright (c) 2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static io.siddhi.query.api.definition.Attribute.Type.DOUBLE;
import static io.siddhi.query.api.definition.Attribute.Type.FLOAT;
import static io.siddhi.query.api.definition.Attribute.Type.INT;
import static io.siddhi.query.api.definition.Attribute.Type.LONG;

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
    private Map<Attribute.Type, ArrayList> arrayListMap = new HashMap<>();
    private Attribute.Type type;

    @Override
    protected StateFactory<ExtensionState> init(ExpressionExecutor[] attributeExpressionExecutors,
                                                ProcessingMode processingMode,
                                                boolean outputExpectsExpiredEvents,
                                                ConfigReader configReader,
                                                SiddhiQueryContext siddhiQueryContext) {
        if (attributeExpressionExecutors.length != 1) {
            throw new OperationNotSupportedException("Median aggregator has to have exactly 1 " +
                    "parameter, currently " + attributeExpressionExecutors.length + " parameters are " +
                    "provided");
        }

        Attribute.Type type = attributeExpressionExecutors[0].getReturnType();

        if (type == DOUBLE || type == INT || type == FLOAT || type == LONG) {
            this.type = type;
        } else {
            throw new OperationNotSupportedException("Median not supported for " + type);
        }

        arrayListMap.put(DOUBLE, new ArrayList<Double>());
        arrayListMap.put(INT, new ArrayList<Integer>());
        arrayListMap.put(FLOAT, new ArrayList<Float>());
        arrayListMap.put(LONG, new ArrayList<Long>());

        return ExtensionState::new;
    }

    public Attribute.Type getReturnType() {
        return this.type;
    }

    @Override
    public Object processAdd(Object data, ExtensionState state) {
        switch (this.type) {
            case DOUBLE:
                (this.arrayListMap.get(this.type)).add((Double) data);
                break;
            case INT:
                (this.arrayListMap.get(this.type)).add((Integer) data);
                break;
            case FLOAT:
                (this.arrayListMap.get(this.type)).add((Float) data);
                break;
            case LONG:
                (this.arrayListMap.get(this.type)).add((Long) data);
                break;
            default:
                throw new OperationNotSupportedException("Median not supported for " + this.type);
        }
        state.count++;
        state.median = getMedian(state);
        return state.median;
    }

    @Override
    public Object processRemove(Object data, ExtensionState state) {
        (this.arrayListMap.get(this.type)).remove(data);
        state.count--;
        state.median = getMedian(state);
        return state.median;
    }

    private double getMedian(ExtensionState state) {
        Collections.sort(this.arrayListMap.get(this.type));

        int midPointA = state.count / 2;
        if (state.count % 2 == 0) {
            int midPointB = midPointA - 1;
            return ((Double) (this.arrayListMap.get(this.type)).get(midPointA)
                    + (Double) (this.arrayListMap.get(this.type)).get(midPointB)) / 2.0;
        }

        return (Double) (this.arrayListMap.get(this.type)).get(midPointA);
    }

    public Object processAdd(Object[] data, ExtensionState state) {
        return new IllegalStateException("Median cannot process data array, but found " +
                Arrays.deepToString(data));
    }

    public Object processRemove(Object[] data, ExtensionState state) {
        return new IllegalStateException("Median cannot process data array, but found " +
                Arrays.deepToString(data));
    }

    @Override
    public Object reset(ExtensionState state) {
        state.count = 0;
        state.median = 0.0;
        return state.median;
    }

    public void start() {
    }

    public void stop() {
    }

    class ExtensionState extends State {
        private int count = 0;
        private double median;

        @Override
        public boolean canDestroy() {
            return (arrayListMap.get(type)).size() == 0 && count == 0 && median == 0.0;
        }

        @Override
        public Map<String, Object> snapshot() {
            Map<String, Object> state = new HashMap<>();
            state.put("Median", this.median);
            state.put("Count", this.count);
            return state;
        }

        @Override
        public void restore(Map<String, Object> state) {
            this.median = (Double) state.get("Median");
            this.count = (Integer) state.get("Count");
        }
    }
}





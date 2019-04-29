/*
 * Copyright (c) 2019, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * you may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.wso2.extension.siddhi.execution.stats;

import io.siddhi.core.util.snapshot.state.State;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * .
 * @param <T>
 */
public abstract class MedianAggregatorState<T extends Number & Comparable<T>> extends State {

    private static final String KEY_MEDIAN = "Median";
    private static final String KEY_COUNT = "Count";

    private List<T> list = new ArrayList<>();

    private final Map<String, Object> state = new HashMap<>();

    private int count = 0;

    private double median = 0.0;

    private double getMedian() {
        Collections.sort(list);

        int midA = count / 2;
        if (count % 2 == 0) {
            int midB = midA - 1;
            return add(list.get(midA), list.get(midB)) / 2;
        }
        return list.get(midA).doubleValue();
    }

    @SuppressWarnings("unchecked")
    public Object processAdd(Object data) {
        list.add((T) data);
        count++;
        median = getMedian();
        return median;
    }

    @SuppressWarnings("unchecked")
    public Object processRemove(Object data) {
        list.remove((T) data);
        count--;
        median = getMedian();
        return median;
    }

    protected abstract double add(T a, T b);

    @Override
    public boolean canDestroy() {
        return list.size() == 0 && count == 0 && median == 0.0;
    }

    @Override
    public Map<String, Object> snapshot() {
        state.put(KEY_MEDIAN, this.median);
        state.put(KEY_COUNT, this.count);
        return state;
    }

    @Override
    public void restore(Map<String, Object> state) {
        this.median = (Double) state.get(KEY_MEDIAN);
        this.count = (Integer) state.get(KEY_COUNT);
    }

    public double reset() {
        list = new ArrayList<>();
        count = 0;
        median = 0.0;
        return median;
    }
}

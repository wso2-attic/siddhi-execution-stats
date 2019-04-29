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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * The MedianAggregator class holds the necessary implementations to manipulate
 * the median calculation and revel corresponding data to needed classes.
 *
 * @param <T> the comparable and mathematically operable type.
 */
public abstract class MedianAggregator<T extends Number & Comparable<T>> {

    private List<T> list = new ArrayList<>();
    private int count = 0;
    private double median = 0.0;

    private double calculateMedian() {
        Collections.sort(list);

        int midA = count / 2;
        if (count % 2 == 0) {
            int midB = midA - 1;
            return add(list.get(midA), list.get(midB)) / 2;
        }
        return list.get(midA).doubleValue();
    }

    @SuppressWarnings("unchecked")
    Object processAdd(Object data) {
        list.add((T) data);
        count++;
        median = calculateMedian();
        return median;
    }

    @SuppressWarnings("unchecked")
    Object processRemove(Object data) {
        list.remove((T) data);
        count--;
        median = calculateMedian();
        return median;
    }

    /**
     * All the subclasses of Number does not support '+' and checking
     * types at compile type could be performance hungry hence keeping
     * abstract is necessary.
     *
     * @param a the first number
     * @param b the second number
     * @return the summation of a and b
     */
    protected abstract double add(T a, T b);

    double reset() {
        list = new ArrayList<>();
        count = 0;
        median = 0.0;
        return median;
    }

    int getCount() {
        return count;
    }

    void setCount(int count) {
        this.count = count;
    }

    double getMedian() {
        return median;
    }

    void setMedian(double median) {
        this.median = median;
    }

    int getValueCount() {
        return list.size();
    }
}

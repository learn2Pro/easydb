package org.learn2pro.easydb.storage.stats;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import java.util.List;
import java.util.Objects;
import org.learn2pro.easydb.storage.Predicate.Op;

/**
 * A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram implements Histogram<Integer> {

    /**
     * the range array
     */
    private Range[] ranges;
    /**
     * the histogram array
     */
    private int[] data;
    /**
     * the min value in histogram
     */
    private int minValue;
    /**
     * the max value in histogram
     */
    private int maxValue;
    /**
     * the bucket size
     */
    private int buckets;
    /**
     * the total size in table
     */
    private int ntups;

    /**
     * Create a new IntHistogram.
     *
     * This IntHistogram should maintain a histogram of integer values that it receives. It should split the histogram
     * into "buckets" buckets.
     *
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     *
     * Your implementation should use space and have execution time that are both constant with respect to the number of
     * values being histogrammed.  For example, you shouldn't simply store every value that you see in a sorted list.
     *
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
        // some code goes here
        Preconditions.checkArgument(max >= min, "max value must greater than min value!");
        this.maxValue = max;
        this.minValue = min;
        this.buckets = buckets;
        this.ntups = 0;
        this.ranges = generateRanges();
        this.data = new int[this.ranges.length];
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     *
     * @param v Value to add to the histogram
     */
    public void addValue(Integer v) {
        // some code goes here
        int idx = getRangeIndex(v);
        if (idx < 0) {
            System.out.println(String.format("the value:%s is not in range!", v));
        } else {
            this.data[idx] += 1;
            this.ntups += 1;
        }
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     *
     * For example, if "op" is "GREATER_THAN" and "v" is 5, return your estimate of the fraction of elements that are
     * greater than 5.
     *
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Op op, Integer v) {
        // some code goes here
        switch (op) {
            case EQUALS:
                return selectiveOfOne(v);
            case NOT_EQUALS:
                return 1.0 - selectiveOfOne(v);
            case GREATER_THAN:
                return selectiveOfRange(v + 1, maxValue);
            case GREATER_THAN_OR_EQ:
                return selectiveOfRange(v, maxValue);
            case LESS_THAN:
                return selectiveOfRange(minValue, v - 1);
            case LESS_THAN_OR_EQ:
                return selectiveOfRange(minValue, v);
            default:
                return -1.0;
        }
    }

    private double selectiveOfOne(int v) {
        int idx = getRangeIndex(v);
        if (idx < 0) {
            return 0.0;
        }
        Range r = ranges[idx];
        int height = data[idx];
        return (height * 1.0 / r.gap()) / ntups;
    }

    private double selectiveOfRange(int index) {
        int height = data[index];
        return height * 1.0 / ntups;
    }

    private double selectiveOfRange(int left, int right) {
        if (right < left) {
            return 0.0;
        } else if (right == left) {
            return selectiveOfOne(left);
        } else {
            left = Math.max(left, minValue);
            right = Math.min(right, maxValue);
            double answer = 0.0;
            for (int i = 0; i < ranges.length; i++) {
                Range current = ranges[i];
                //not in input range
                if (current.right < left || current.left > right) {
                    continue;
                }
                //all in input range
                else if (current.left >= left && current.right <= right) {
                    answer += selectiveOfRange(i);
                }
                //partial in input left range
                else if (left >= current.left) {
                    for (int j = left; j < Math.min(current.right, right); j++) {
                        answer += selectiveOfOne(j);
                    }
                }
                //partial in input right range
                else if (right <= current.right) {
                    for (int j = Math.max(left, current.left); j < current.right; j++) {
                        answer += selectiveOfOne(j);
                    }
                }
            }
            return answer;
        }

    }

    /**
     * @return the average selectivity of this histogram.
     *
     *         This is not an indispensable method to implement the basic join optimization. It may be needed if you
     *         want to implement a more efficient optimization
     */
    public double avgSelectivity() {
        // some code goes here
        return 1.0;
    }

    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        return String.format("max:%s,min:%s,buckets:%s,ranges:%s,data:%s", maxValue, minValue, buckets, ranges, data);
    }

    private Range[] generateRanges() {
        int gap = getGapByBucket();
        List<Range> ranges = Lists.newArrayList();
        for (int i = minValue; i <= maxValue; i += gap) {
            ranges.add(new Range(i, Math.min(maxValue + 1, i + gap)));
        }
        return ranges.toArray(new Range[0]);
    }

    private int getRangeIndex(int value) {
        if (value < minValue || value > maxValue) {
            return -1;
        } else {
            return (value - minValue) / getGapByBucket();
        }
    }

    private int getGapByBucket() {
        return (int) Math.ceil((1.0 * (this.maxValue + 1 - this.minValue)) / buckets);
    }

    /**
     * range of [left,right)
     */
    static class Range implements Comparable<Range> {

        private int left;
        private int right;

        public Range(int left, int right) {
            this.left = left;
            this.right = right;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Range range = (Range) o;
            return left == range.left && right == range.right;
        }

        @Override
        public int hashCode() {
            return Objects.hash(left, right);
        }

        public int gap() {
            return right - left;
        }

        @Override
        public int compareTo(Range other) {
            if (left < other.left) {
                return -1;
            } else if (left > other.left) {
                return 1;
            } else {
                //compare right
                if (right < other.right) {
                    return -1;
                } else if (right > other.right) {
                    return 1;
                } else {
                    return 0;
                }
            }
        }
    }
}

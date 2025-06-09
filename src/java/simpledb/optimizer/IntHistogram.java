package simpledb.optimizer;

import simpledb.execution.Predicate.*;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * A class to represent a fixed-width histogram over a single integer-based field.
 */
@SuppressWarnings("AlibabaLowerCamelCaseVariableNaming")
public class IntHistogram {
    private int[] bins;
    private int min;
    private int max;
    private int ntups;
    private int buckets;
    /**
     * width of each bucket
     */
    private double width;
    private Set<Integer> uniqueValue;


    /**
     * Create a new IntHistogram.
     * <p>
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * <p>
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * <p>
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't
     * simply store every value that you see in a sorted list.
     *
     * @param buckets The number of buckets to split the input value into.
     * @param min     The minimum integer value that will ever be passed to this class for histogramming
     * @param max     The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
        this.min = min;
        this.max = max;
        this.buckets = buckets;
        bins = new int[buckets];
        ntups = 0;
        width = (max - min + 1) * 1.0 / buckets;
        uniqueValue = new HashSet<>();
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     *
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        int idx = (int) ((v - min) / this.width);
        this.bins[idx]++;
        this.ntups++;
        uniqueValue.add(v);
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * <p>
     * For example, if "op" is "GREATER_THAN" and "v" is 5,
     * return your estimate of the fraction of elements that are greater than 5.
     *
     * @param op Operator
     * @param v  Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Op op, int v) {
        switch (op) {
            case EQUALS:
                return estimateEqual(v);
            case LESS_THAN:
                return estimateLessThan(v);
            case GREATER_THAN:
                return estimateGreaterThan(v);
            case LESS_THAN_OR_EQ:
                return estimateLessThan(v) + estimateEqual(v);
            case GREATER_THAN_OR_EQ:
                return estimateGreaterThan(v) + estimateEqual(v);
            case NOT_EQUALS:
                return 1 - estimateEqual(v);
            default:
                throw new UnsupportedOperationException("Unsupported Operator");
        }
    }

    private int getIndex(int v) {
        if (v > max || v < min) {
            throw new ArrayIndexOutOfBoundsException("Value out of bound");
        }
        return (int) ((v - min) / this.width);
    }


    private double estimateEqual(int v) {
        if (v > max || v < min) {
            return 0.0;
        }
        int idx = getIndex(v);
        double height = bins[idx] * 1.0;
        return height / width / ntups;
    }

    private double estimateGreaterThan(int v) {
        if (v >= max) {
            return 0;
        }
        if (v < min) {
            return 1;
        }
        int idx = this.getIndex(v);
        double h_b = this.bins[idx] * 1.0;

        double b_f = (h_b / (ntups * 1.0));
        double b_right = (idx + 1) * width + min;
        double b_part = (b_right - v * 1.0) / width;

        double ntupsGreater = 0;
        for (int i = idx + 1; i < buckets; i += 1) {
            ntupsGreater += this.bins[i];
        }
        return b_f * b_part + ntupsGreater / ntups;
    }


    private double estimateLessThan(int v) {
        if (v > max) {
            return 1;
        }
        if (v <= min) {
            return 0;
        }
        int idx = this.getIndex(v);
        double h_b = this.bins[idx] * 1.0;

        double b_f = (h_b / (ntups * 1.0));
        double b_left = idx * width + min;
        double b_part = (v * 1.0 - b_left) / width;

        double ntupsLess = 0;
        for (int i = 0; i < idx; i += 1) {
            ntupsLess += this.bins[i];
        }
        return b_f * b_part + ntupsLess / ntups;
    }

    /**
     * @return the average selectivity of this histogram.
     * <p>
     * This is not an indispensable method to implement the basic
     * join optimization. It may be needed if you want to
     * implement a more efficient optimization
     */
    public double avgSelectivity() {
        return 1.0 / uniqueValue.size();
    }

    /**
     * @return A string describing this histogram, for debugging purposes
     */
    @Override
    public String toString() {
        return "IntHistogram{" +
                "slots=" + Arrays.toString(bins) +
                ", min=" + min +
                ", max=" + max +
                ", ntups=" + ntups +
                ", width=" + width +
                '}';
    }
}

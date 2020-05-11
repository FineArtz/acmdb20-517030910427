package simpledb;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private int buckets, min, max;
    private double width;
    private int[] histogram;
    private int ntups;

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
        this.buckets = buckets;
    	this.min = min;
    	this.max = max;
    	this.width = (double) (max - min + 1) / buckets;
    	this.histogram = new int[buckets];
    	this.ntups = 0;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	++histogram[bucketId(v, false)];
    	++ntups;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
        int id = bucketId(v, true);
        double ret = 0.0;
        double err = 0.0;
        double lower = min + id * width;
        double upper = min + (id + 1) * width;
        switch (op) {
            case LESS_THAN_OR_EQ:
                err = histogram[id] / (Math.floor(upper) - Math.ceil(lower) + Math.ceil(upper - (int) upper));
            case LESS_THAN:
                if (v <= min) {
                    ret = 0.0;
                }
                else if (v > max) {
                    ret = 1.0;
                }
                else {
                    ret += (v - lower) / width * histogram[id] + err;
                    for (int i = 0; i < id; ++i) {
                        ret += histogram[i];
                    }
                    ret /= ntups;
                }
                break;
            case GREATER_THAN:
                ret = 1 - estimateSelectivity(Predicate.Op.LESS_THAN_OR_EQ, v);
                break;
            case GREATER_THAN_OR_EQ:
                ret = 1 - estimateSelectivity(Predicate.Op.LESS_THAN, v);
                break;
            case EQUALS:
                ret = estimateSelectivity(Predicate.Op.LESS_THAN_OR_EQ, v)
                        - estimateSelectivity(Predicate.Op.LESS_THAN, v);
                break;
            case NOT_EQUALS:
                ret = 1 - estimateSelectivity(Predicate.Op.EQUALS, v);
                break;
            case LIKE:
                break;
        }
        return ret;
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        double ret = 0.0;
        for (int i = 0; i < buckets; ++i) {
            ret += histogram[i];
        }
        return ret / ntups;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        return String.format("IntHistogram(buckets: %d, min: %d, max: %d)", buckets, min, max);
    }

    private int bucketId(int v, boolean project) {
        if (project) {
            v = Math.max(v, min);
            v = Math.min(v, max);
        }
        if (v < min || v > max) {
            throw new IllegalArgumentException(String.format(
                    "Value %d out of range in IntHistogram(min %d, max %d).", v, min, max));
        }
        return (int)((v - min) / width);
    }
}

package simpledb;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing lab1, lab2 and lab3.
 */
public class TableStats {

    private static final ConcurrentHashMap<String, TableStats> statsMap = new ConcurrentHashMap<String, TableStats>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(HashMap<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        } catch (SecurityException e) {
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    private static final int NUM_HIST_BINS = 160;

    private int ioCostPerPage;
    private HashMap<Integer, IntHistogram> intHistMap = new HashMap<>();
    private HashMap<Integer, StringHistogram> strHistMap = new HashMap<>();
    private DbFile file;
    private int numTuples;

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        this.ioCostPerPage = ioCostPerPage;
        this.file = Database.getCatalog().getDatabaseFile(tableid);
        TupleDesc tupleDesc = file.getTupleDesc();
        this.numTuples = 0;

        // The first scan to calculate minima and maxima.
        int numFields = tupleDesc.numFields();
        int[] mins = new int[numFields];
        int[] maxs = new int[numFields];
        Arrays.fill(mins, 0x7fffffff);
        Arrays.fill(maxs, 0x80000000);
        TransactionId tid = new TransactionId();
        SeqScan seqScan = new SeqScan(tid, tableid);
        Type[] types = new Type[numFields];
        for (int i = 0; i < numFields; ++i) {
            types[i] = tupleDesc.getFieldType(i);
        }
        try {
            seqScan.open();
            while (seqScan.hasNext()) {
                ++numTuples;
                Tuple tuple = seqScan.next();
                for (int i = 0; i < numFields; ++i) {
                    if (types[i] == Type.STRING_TYPE) {
                        mins[i] = 0;
                        maxs[i] = 0;
                        continue;
                    }
                    int fieldVal = ((IntField) tuple.getField(i)).getValue();
                    mins[i] = Math.min(mins[i], fieldVal);
                    maxs[i] = Math.max(maxs[i], fieldVal);
                }
            }
            seqScan.close();
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        // Construct histograms.
        for (int i = 0; i < numFields; ++i) {
            if (types[i] == Type.INT_TYPE) {
                intHistMap.put(i, new IntHistogram(NUM_HIST_BINS, mins[i], maxs[i]));
            }
            else {
                strHistMap.put(i, new StringHistogram(NUM_HIST_BINS));
            }
        }

        // The second scan to populate the counts.
        try {
            seqScan.rewind();
            while (seqScan.hasNext()) {
                Tuple tuple = seqScan.next();
                for (int i = 0; i < numFields; ++i) {
                    if (types[i] == Type.INT_TYPE) {
                        intHistMap.get(i).addValue(((IntField) tuple.getField(i)).getValue());
                    }
                    else {
                        strHistMap.get(i).addValue(((StringField) tuple.getField(i)).getValue());
                    }
                }
            }
            seqScan.close();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */
    double estimateScanCost() {
        return ioCostPerPage * ((HeapFile) file).numPages();
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    int estimateTableCardinality(double selectivityFactor) {
        return (int) (numTuples * selectivityFactor);
    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     * */
    double avgSelectivity(int field, Predicate.Op op) {
        return 1.0;
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        double ret;
        if (intHistMap.containsKey(field)) {
            ret = intHistMap.get(field).estimateSelectivity(op, ((IntField) constant).getValue());
        }
        else {
            ret = strHistMap.get(field).estimateSelectivity(op, ((StringField) constant).getValue());
        }
        return ret;
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        return numTuples;
    }

}

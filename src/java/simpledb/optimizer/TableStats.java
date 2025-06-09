package simpledb.optimizer;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.execution.Predicate;
import simpledb.execution.SeqScan;
import simpledb.storage.*;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query.
 * <p>
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {

    private static final ConcurrentMap<String, TableStats> STATS_MAP = new ConcurrentHashMap<>();

    static final int IO_COST_PEERAGE = 1000;

    private int ioCostPerPage;
    private DbFile dbFile;
    private TupleDesc tupleDesc;
    private int numPages;
    private int numTuples;

    private Map<Integer, IntHistogram> intHistogramMap;
    private Map<Integer, StringHistogram> stringHistogramMap;

    public static TableStats getTableStats(String tablename) {
        return STATS_MAP.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        STATS_MAP.put(tablename, stats);
    }

    public static void setStatsMap(Map<String, TableStats> s) {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("STATS_MAP");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException | IllegalAccessException | IllegalArgumentException | SecurityException e) {
            System.err.println(e.getMessage());
        }
    }

    public static Map<String, TableStats> getStatsMap() {
        return STATS_MAP;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IO_COST_PEERAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 32;

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     *
     * @param tableid       The table over which to compute statistics
     * @param ioCostPerPage The cost per page of IO. This doesn't differentiate between
     *                      sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        this.ioCostPerPage = ioCostPerPage;
        this.dbFile = Database.getCatalog().getDatabaseFile(tableid);
        this.tupleDesc = dbFile.getTupleDesc();
        this.numPages = ((HeapFile) dbFile).numPages();
        this.intHistogramMap = new HashMap<>();
        this.stringHistogramMap = new HashMap<>();

        int[] minVals = new int[tupleDesc.numFields()];
        int[] maxVals = new int[tupleDesc.numFields()];
        Arrays.fill(maxVals, Integer.MIN_VALUE);
        Arrays.fill(minVals, Integer.MAX_VALUE);

        // count all tuples and calculate min&max value
        TransactionId tid = new TransactionId();
        SeqScan ss = new SeqScan(tid, tableid);
        int numTuples = 0;
        try {
            ss.open();
            while (ss.hasNext()) {
                Tuple tuple = ss.next();
                numTuples += 1;
                for (int i = 0; i < tupleDesc.numFields(); i++) {
                    if (tupleDesc.getFieldType(i) == Type.INT_TYPE) {
                        int val = ((IntField) tuple.getField(i)).getValue();
                        minVals[i] = Math.min(minVals[i], val);
                        maxVals[i] = Math.max(maxVals[i], val);
                    }
                }
            }
        } catch (DbException | TransactionAbortedException e) {
            System.err.println(e.getMessage());
        }
        this.numTuples = numTuples;

        // initialize histograms
        for (int i = 0; i < tupleDesc.numFields(); i++) {
            if (tupleDesc.getFieldType(i) == Type.INT_TYPE) {
                intHistogramMap.put(i, new IntHistogram(
                        NUM_HIST_BINS, minVals[i], maxVals[i]
                ));
            } else if (tupleDesc.getFieldType(i) == Type.STRING_TYPE) {
                stringHistogramMap.put(i, new StringHistogram(NUM_HIST_BINS));
            }
        }

        // add values to histograms
        try {
            ss.rewind();
            while (ss.hasNext()) {
                Tuple tuple = ss.next();
                for (int i = 0; i < tupleDesc.numFields(); i++) {
                    if (tupleDesc.getFieldType(i) == Type.INT_TYPE) {
                        int val = ((IntField) tuple.getField(i)).getValue();
                        intHistogramMap.get(i).addValue(val);
                    } else if (tupleDesc.getFieldType(i) == Type.STRING_TYPE) {
                        String val = ((StringField) tuple.getField(i)).getValue();
                        stringHistogramMap.get(i).addValue(val);
                    }
                }
            }
        } catch (DbException | TransactionAbortedException e) {
            System.err.println(e.getMessage());
        } finally {
            ss.close();
        }
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * <p>
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     *
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        return numPages * ioCostPerPage;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     *
     * @param selectivityFactor The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     * selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        return (int) (totalTuples() * selectivityFactor);
    }

    /**
     * The average selectivity of the field under op.
     *
     * @param field the index of the field
     * @param op    the operator in the predicate
     *              The semantic of the method is that, given the table, and then given a
     *              tuple, of which we do not know the value of the field, return the
     *              expected selectivity. You may estimate this value from the histograms.
     */
    public double avgSelectivity(int field, Predicate.Op op) {
        if (tupleDesc.getFieldType(field) == Type.INT_TYPE) {
            return intHistogramMap.get(field).avgSelectivity();
        } else  {
            return stringHistogramMap.get(field).avgSelectivity();
        }
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     *
     * @param field    The field over which the predicate ranges
     * @param op       The logical operation in the predicate
     * @param constant The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     * predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        if (tupleDesc.getFieldType(field) == Type.INT_TYPE) {
            return intHistogramMap.get(field).estimateSelectivity(op, ((IntField) constant).getValue());
        } else  {
            return stringHistogramMap.get(field).estimateSelectivity(op, ((StringField) constant).getValue());
        }
    }

    /**
     * return the total number of tuples in this table
     */
    public int totalTuples() {
        return numTuples;
    }

}

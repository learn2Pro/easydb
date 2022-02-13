package org.learn2pro.easydb.storage.stats;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import org.learn2pro.easydb.storage.Database;
import org.learn2pro.easydb.storage.DbException;
import org.learn2pro.easydb.storage.DbFile;
import org.learn2pro.easydb.storage.DbFileIterator;
import org.learn2pro.easydb.storage.HeapFile;
import org.learn2pro.easydb.storage.Predicate.Op;
import org.learn2pro.easydb.storage.TransactionAbortedException;
import org.learn2pro.easydb.storage.TransactionId;
import org.learn2pro.easydb.storage.Tuple;
import org.learn2pro.easydb.storage.TupleDesc;
import org.learn2pro.easydb.storage.Type;
import org.learn2pro.easydb.storage.common.Field;
import org.learn2pro.easydb.storage.common.IntField;
import org.learn2pro.easydb.storage.common.StringField;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a query.
 *
 * This class is not needed in implementing lab1 and lab2.
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

    public static void setStatsMap(HashMap<String, TableStats> s) {
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
     * Number of bins for the histogram. Feel free to increase this value over 100, though our tests assume that you
     * have at least 100 bins in your histograms.
     */
    static final int NUM_HIST_BINS = 100;

    /**
     * the stats of field in table
     */
    private Histogram[] stats;
    /**
     * the cost for scan table
     */
    private int ioCostPerPage;
    /**
     * the specified table
     */
    private int tableId;
    /**
     * the tuple desc
     */
    private TupleDesc tupleDesc;
    /**
     * the tatal tuple size in table
     */
    private int ntups;
    /**
     * only int field contains value this size equals to tupleDesc field size
     */
    private int[] minOfIntField;
    /**
     * only int field contains value this size equals to tupleDesc field size
     */
    private int[] maxOfIntField;

    /**
     * Create a new TableStats object, that keeps track of statistics on each column of a table
     *
     * @param tableid The table over which to compute statistics
     * @param ioCostPerPage The cost per page of IO. This doesn't differentiate between sequential-scan IO and
     *         disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        // some code goes here
        TupleDesc td = Database.getCatalog().getTupleDesc(tableid);
        this.stats = new Histogram[td.numFields()];
        this.tableId = tableid;
        this.tupleDesc = td;
        this.minOfIntField = new int[td.numFields()];
        this.maxOfIntField = new int[td.numFields()];
        this.ioCostPerPage = ioCostPerPage;
//        for (int i = 0; i < this.minOfIntField.length; i++) {
//            minOfIntField[i] = Integer.MIN_VALUE;
//        }
//        for (int i = 0; i < this.maxOfIntField.length; i++) {
//            maxOfIntField[i] = Integer.MAX_VALUE;
//        }
        this.stats = new Histogram[td.numFields()];
        //record min/max/total
        recordMinMaxAndTotal();
        //create histogram
        for (int i = 0; i < td.numFields(); i++) {
            switch (td.getFieldType(i)) {
                case INT_TYPE:
                    this.stats[i] = new IntHistogram(NUM_HIST_BINS, minOfIntField[i], maxOfIntField[i]);
                    break;
                case STRING_TYPE:
                    this.stats[i] = new StringHistogram(NUM_HIST_BINS);
                    break;
            }
        }
        //record histogram
        recordHistogram();

    }

    private void recordHistogram() {
        scanTableOnce(this.tableId, t -> {
            for (int i = 0; i < this.tupleDesc.numFields(); i++) {
                switch (this.tupleDesc.getFieldType(i)) {
                    case INT_TYPE:
                        stats[i].addValue(((IntField) t.getField(i)).getValue());
                        break;
                    case STRING_TYPE:
                        stats[i].addValue(((IntField) t.getField(i)).getValue());
                        break;
                }
            }
        });
    }

    private void recordMinMaxAndTotal() {
        scanTableOnce(this.tableId, t -> {
            this.ntups += 1;
            for (int i = 0; i < this.tupleDesc.numFields(); i++) {
                Type typo = this.tupleDesc.getFieldType(i);
                if (typo == Type.INT_TYPE) {
                    minOfIntField[i] = Math.min(minOfIntField[i], ((IntField) t.getField(i)).getValue());
                    maxOfIntField[i] = Math.max(maxOfIntField[i], ((IntField) t.getField(i)).getValue());
                }
            }
        });
    }

    private void scanTableOnce(int tableid, Consumer<Tuple> action) {
        DbFile dbFile = Database.getCatalog().getDatabaseFile(tableid);
        try {
            DbFileIterator iterator = dbFile.iterator(new TransactionId());
            iterator.open();
            while (iterator.hasNext()) {
                Tuple t = iterator.next();
                action.accept(t);
            }
        } catch (DbException | TransactionAbortedException e) {
            e.printStackTrace();
            System.out.println(String.format("get stats from table:%s failed!", tableid));
        }
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost to read a page is costPerPageIO. You
     * can assume that there are no seeks and that no pages are in the buffer pool.
     *
     * Also, assume that your hard drive can only read entire pages at once, so if the last page of the table only has
     * one tuple on it, it's just as expensive to read as a full page. (Most real hard drives can't efficiently address
     * regions smaller than a page at a time.)
     *
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        // some code goes here
        DbFile dbFile = Database.getCatalog().getDatabaseFile(tableId);
        if (dbFile instanceof HeapFile) {
            return ioCostPerPage * ((HeapFile) dbFile).numPages();
        } else {
            throw new UnsupportedOperationException("not support estimate this dbfile cost now!");
        }
    }

    /**
     * This method returns the number of tuples in the relation, given that a predicate with selectivity
     * selectivityFactor is applied.
     *
     * @param selectivityFactor The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        // some code goes here
        return (int) (totalTuples() * selectivityFactor);
    }

    /**
     * The average selectivity of the field under op.
     *
     * @param field the index of the field
     * @param op the operator in the predicate The semantic of the method is that, given the table, and then
     *         given a tuple, of which we do not know the value of the field, return the expected selectivity. You may
     *         estimate this value from the histograms.
     */
    public double avgSelectivity(int field, Op op) {
        // some code goes here
        return 1.0;
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the table.
     *
     * @param field The field over which the predicate ranges
     * @param op The logical operation in the predicate
     * @param constant The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the predicate
     */
    public double estimateSelectivity(int field, Op op, Field constant) {
        // some code goes here
        if (constant instanceof IntField) {
            return this.stats[field].estimateSelectivity(op, ((IntField) constant).getValue());
        } else if (constant instanceof StringField) {
            return this.stats[field].estimateSelectivity(op, ((StringField) constant).getValue());
        } else {
            return -1.0;
        }

    }

    /**
     * return the total number of tuples in this table
     */
    public int totalTuples() {
        // some code goes here
        return ntups;
    }

}

package org.learn2pro.easydb.storage.tests;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.Lists;
import junit.framework.JUnit4TestAdapter;
import org.junit.Test;
import org.learn2pro.easydb.storage.Predicate;
import org.learn2pro.easydb.storage.Predicate.Op;
import org.learn2pro.easydb.storage.StringField;
import org.learn2pro.easydb.storage.Tuple;
import org.learn2pro.easydb.storage.TupleDesc;
import org.learn2pro.easydb.storage.TupleDesc.TDItem;
import org.learn2pro.easydb.storage.Type;
import org.learn2pro.easydb.storage.Utility;
import org.learn2pro.easydb.storage.tests.systemtest.SimpleDbTestBase;

public class PredicateTest extends SimpleDbTestBase {

    /**
     * Unit test for Predicate.filter()
     */
    @Test
    public void filter() {
        int[] vals = new int[]{-1, 0, 1};

        for (int i : vals) {
            Predicate p = new Predicate(0, Predicate.Op.EQUALS, TestUtil.getField(i));
            assertFalse(p.filter(Utility.getHeapTuple(i - 1)));
            assertTrue(p.filter(Utility.getHeapTuple(i)));
            assertFalse(p.filter(Utility.getHeapTuple(i + 1)));
        }

        for (int i : vals) {
            Predicate p = new Predicate(0, Predicate.Op.GREATER_THAN,
                    TestUtil.getField(i));
            assertFalse(p.filter(Utility.getHeapTuple(i - 1)));
            assertFalse(p.filter(Utility.getHeapTuple(i)));
            assertTrue(p.filter(Utility.getHeapTuple(i + 1)));
        }

        for (int i : vals) {
            Predicate p = new Predicate(0, Predicate.Op.GREATER_THAN_OR_EQ,
                    TestUtil.getField(i));
            assertFalse(p.filter(Utility.getHeapTuple(i - 1)));
            assertTrue(p.filter(Utility.getHeapTuple(i)));
            assertTrue(p.filter(Utility.getHeapTuple(i + 1)));
        }

        for (int i : vals) {
            Predicate p = new Predicate(0, Predicate.Op.LESS_THAN,
                    TestUtil.getField(i));
            assertTrue(p.filter(Utility.getHeapTuple(i - 1)));
            assertFalse(p.filter(Utility.getHeapTuple(i)));
            assertFalse(p.filter(Utility.getHeapTuple(i + 1)));
        }

        for (int i : vals) {
            Predicate p = new Predicate(0, Predicate.Op.LESS_THAN_OR_EQ,
                    TestUtil.getField(i));
            assertTrue(p.filter(Utility.getHeapTuple(i - 1)));
            assertTrue(p.filter(Utility.getHeapTuple(i)));
            assertFalse(p.filter(Utility.getHeapTuple(i + 1)));
        }
    }

    @Test
    public void testLike() {
        String[] vals = new String[]{"abc", "cba", "xyz"};
        TupleDesc td = new TupleDesc(Lists.newArrayList(new TDItem(Type.STRING_TYPE, "name")));
        Predicate p = new Predicate(0, Op.LIKE, TestUtil.getField(vals[0]));
        Tuple tuple = new Tuple(td);
        tuple.setField(0, new StringField("abcd"));
        assertTrue(p.filter(tuple));
        p = new Predicate(0, Op.LIKE, TestUtil.getField(vals[1]));
        tuple.setField(0, new StringField("cbad"));
        assertTrue(p.filter(tuple));
        p = new Predicate(0, Op.LIKE, TestUtil.getField(vals[2]));
        tuple.setField(0, new StringField("abcd"));
        assertFalse(p.filter(tuple));
    }

    /**
     * JUnit suite target
     */
    public static junit.framework.Test suite() {
        return new JUnit4TestAdapter(PredicateTest.class);
    }
}


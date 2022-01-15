package org.learn2pro.easydb.storage.tests;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import junit.framework.JUnit4TestAdapter;
import org.junit.Test;
import org.learn2pro.easydb.storage.Predicate;
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

    /**
     * JUnit suite target
     */
    public static junit.framework.Test suite() {
        return new JUnit4TestAdapter(PredicateTest.class);
    }
}


package org.learn2pro.easydb.storage.tests;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import junit.framework.JUnit4TestAdapter;
import org.junit.Test;
import org.learn2pro.easydb.storage.JoinPredicate;
import org.learn2pro.easydb.storage.Predicate;
import org.learn2pro.easydb.storage.Utility;
import org.learn2pro.easydb.storage.tests.systemtest.SimpleDbTestBase;

public class JoinPredicateTest extends SimpleDbTestBase {

    /**
     * Unit test for JoinPredicate.filter()
     */
    @Test
    public void filterVaryingVals() {
        int[] vals = new int[]{-1, 0, 1};

        for (int i : vals) {
            JoinPredicate p = new JoinPredicate(0,
                    Predicate.Op.EQUALS, 0);
            assertFalse(p.filter(Utility.getHeapTuple(i), Utility.getHeapTuple(i - 1)));
            assertTrue(p.filter(Utility.getHeapTuple(i), Utility.getHeapTuple(i)));
            assertFalse(p.filter(Utility.getHeapTuple(i), Utility.getHeapTuple(i + 1)));
        }

        for (int i : vals) {
            JoinPredicate p = new JoinPredicate(0,
                    Predicate.Op.GREATER_THAN, 0);
            assertTrue(p.filter(Utility.getHeapTuple(i), Utility.getHeapTuple(i - 1)));
            assertFalse(p.filter(Utility.getHeapTuple(i), Utility.getHeapTuple(i)));
            assertFalse(p.filter(Utility.getHeapTuple(i), Utility.getHeapTuple(i + 1)));
        }

        for (int i : vals) {
            JoinPredicate p = new JoinPredicate(0,
                    Predicate.Op.GREATER_THAN_OR_EQ, 0);
            assertTrue(p.filter(Utility.getHeapTuple(i), Utility.getHeapTuple(i - 1)));
            assertTrue(p.filter(Utility.getHeapTuple(i), Utility.getHeapTuple(i)));
            assertFalse(p.filter(Utility.getHeapTuple(i), Utility.getHeapTuple(i + 1)));
        }

        for (int i : vals) {
            JoinPredicate p = new JoinPredicate(0,
                    Predicate.Op.LESS_THAN, 0);
            assertFalse(p.filter(Utility.getHeapTuple(i), Utility.getHeapTuple(i - 1)));
            assertFalse(p.filter(Utility.getHeapTuple(i), Utility.getHeapTuple(i)));
            assertTrue(p.filter(Utility.getHeapTuple(i), Utility.getHeapTuple(i + 1)));
        }

        for (int i : vals) {
            JoinPredicate p = new JoinPredicate(0,
                    Predicate.Op.LESS_THAN_OR_EQ, 0);
            assertFalse(p.filter(Utility.getHeapTuple(i), Utility.getHeapTuple(i - 1)));
            assertTrue(p.filter(Utility.getHeapTuple(i), Utility.getHeapTuple(i)));
            assertTrue(p.filter(Utility.getHeapTuple(i), Utility.getHeapTuple(i + 1)));
        }
    }

    /**
     * JUnit suite target
     */
    public static junit.framework.Test suite() {
        return new JUnit4TestAdapter(JoinPredicateTest.class);
    }
}


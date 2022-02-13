package org.learn2pro.easydb.storage.stats;

import org.learn2pro.easydb.storage.Predicate;

public interface Histogram<T> {

    double estimateSelectivity(Predicate.Op op, T v);

    void addValue(T v);
}

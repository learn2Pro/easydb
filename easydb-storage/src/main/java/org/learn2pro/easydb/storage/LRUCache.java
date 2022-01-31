package org.learn2pro.easydb.storage;

import java.util.LinkedHashMap;

public class LRUCache<K, T> extends LinkedHashMap<K, T> {

    private int capacity;

    /**
     * Constructs an empty insertion-ordered <tt>LinkedHashMap</tt> instance with the default initial capacity (16) and
     * load factor (0.75).
     */
    public LRUCache(int capacity) {
        this.capacity = capacity;
    }

    public K evictNode() {
        return this.entrySet().iterator().next().getKey();
    }
}

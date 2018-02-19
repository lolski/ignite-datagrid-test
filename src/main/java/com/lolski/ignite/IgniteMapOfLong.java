package com.lolski.ignite;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.transactions.Transaction;

import java.util.Optional;

public class IgniteMapOfLong {
    private IgniteCache<String, Long> igniteCache;

    public IgniteMapOfLong(IgniteCache<String, Long> igniteCache) {
        this.igniteCache = igniteCache;
    }

    public long incrementByTx(IgniteTransactions t, String key, long incrementBy) {
        try (Transaction tx = t.txStart()) {
            long current = Optional.ofNullable(igniteCache.get(key)).orElse(0L);
            long incremented = current + incrementBy;
            igniteCache.put(key, incremented);
            tx.commit();
            return incremented;
        }
    }

    public long get(String key) {
        long value = Optional.ofNullable(igniteCache.get(key)).orElse(0L);
        return value;
    }
}

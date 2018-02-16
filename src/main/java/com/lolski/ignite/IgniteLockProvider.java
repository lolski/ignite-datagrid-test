package com.lolski.ignite;

import org.apache.ignite.IgniteCache;

import java.util.concurrent.locks.Lock;

public class IgniteLockProvider {
    public IgniteCache cache;

    public IgniteLockProvider(IgniteCache cache) {
        this.cache = cache;
    }

    public Lock getLock(String lockName) {
        return cache.lock(lockName);
    }
}

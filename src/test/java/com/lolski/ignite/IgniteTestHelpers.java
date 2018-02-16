package com.lolski.ignite;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMode;

import java.util.SortedSet;

public class IgniteTestHelpers {
    public static IgniteStorage getIgniteStorage() {
        Ignite ignite = getIgnite();
        IgniteCache<String, SortedSet<String>> a = ignite.getOrCreateCache("a");
        IgniteCache<String, SortedSet<String>> b = ignite.getOrCreateCache("b");
        return new IgniteStorage(ignite, new IgniteMultiMap(a), new IgniteMultiMap(b));
    }

    public static Ignite getIgnite() {
        return Ignition.start();
    }

    public static IgniteCache<String, SortedSet<String>> getCache(Ignite ignite, String name) {
        return ignite.getOrCreateCache(name);
    }

}

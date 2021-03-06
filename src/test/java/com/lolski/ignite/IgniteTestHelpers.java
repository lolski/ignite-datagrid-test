package com.lolski.ignite;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;

import java.util.SortedSet;

public class IgniteTestHelpers {
    public static IgniteIndexStorage getIgniteStorage() {
        Ignite ignite = getIgnite();
        IgniteCache<String, SortedSet<String>> a = ignite.getOrCreateCache("a");
        IgniteCache<String, SortedSet<String>> b = ignite.getOrCreateCache("b");
        return new IgniteIndexStorage(ignite, new IgniteMultiMap(a), new IgniteMultiMap(b));
    }

    public static Ignite getIgnite() {
        return Ignition.start();
    }

    public static IgniteCache<String, SortedSet<String>> getCacheMultiMap(Ignite ignite, String name) {
        return ignite.getOrCreateCache(name);
    }

    public static IgniteCache<String, Long> getCacheMapOfLong(Ignite ignite, String name) {
        return ignite.getOrCreateCache(name);
    }
}

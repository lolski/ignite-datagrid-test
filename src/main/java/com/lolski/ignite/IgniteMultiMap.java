package com.lolski.ignite;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.transactions.Transaction;

import java.util.Arrays;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.Optional;

public class IgniteMultiMap {
    public final String NAME;

    private IgniteCache<String, SortedSet<String>> igniteCache = null;

    public IgniteMultiMap(String name) {
        this.NAME = name;
    }

    public IgniteMultiMap getOrCreate(Ignite ignite) {
        igniteCache = ignite.getOrCreateCache(NAME);
        return this;
    }

    public void putOneTx(IgniteTransactions t, String key, String value) {
        try (Transaction tx = t.txStart()) {
            putOne(key, value);
            tx.commit();
        }
    }

    public void putOne(String key, String value) {
        putAll(key, new TreeSet<>(Arrays.asList(value)));
    }

    public void putAll(String key, Set<String> values) {
        SortedSet<String> updatedIndices = getKeyspaceFromCache(igniteCache, key).orElse(new TreeSet<>());
        updatedIndices.addAll(values);
        igniteCache.put(key, updatedIndices);
    }

    public String popOneTx(IgniteTransactions t, String key) {
        try (Transaction tx = t.txStart()) {
           Optional<SortedSet<String>> indicesOpt = getKeyspaceFromCache(igniteCache, key);
           if (indicesOpt.isPresent()) {
               String pop = popAndGetFirstElement(indicesOpt.get());
               igniteCache.put(key, indicesOpt.get());
               tx.commit();
               return pop;
           } else {
               throw new SetDoesNotExistException();
           }
        }
    }

    public Set<String> popAllTx(IgniteTransactions t, String setName) {
        try (Transaction tx = t.txStart()) {
            Optional<SortedSet<String>> indicesOpt = getKeyspaceFromCache(igniteCache, setName);
            if (indicesOpt.isPresent()) {
                SortedSet<String> getAll = igniteCache.get(setName);
                igniteCache.put(setName, new TreeSet<>());
                tx.commit();
                return getAll;
            } else {
                throw new SetDoesNotExistException();
            }
        }
    }

    public Set<String> getAll(String setName) {
        Set<String> all = igniteCache.get(setName);
        return all;
    }

    public Set<String> getKeys() {
        Set<String> keys = new TreeSet<>();
        igniteCache.query(new ScanQuery<>(null)).forEach(e -> keys.add((String) e.getKey()));
        return keys;
    }

    private static Optional<SortedSet<String>> getKeyspaceFromCache(IgniteCache<String, SortedSet<String>> igniteCache, String setName) {
        return Optional.ofNullable(igniteCache.get(setName));
    }

    private String popAndGetFirstElement(SortedSet<String> elements) {
        String pop = elements.first();
        elements.remove(pop);
        return pop;
    }
}


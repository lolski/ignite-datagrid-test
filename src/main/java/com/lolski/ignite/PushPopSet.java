package com.lolski.ignite;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.transactions.Transaction;
import sun.nio.cs.StandardCharsets;

import javax.cache.Cache;
import java.util.*;

// TODO: differentiate between a mapping between
// a) a collection containing a keyspace with no indices to process, and
// b) a collection containing no keyspace
// Optional<?> getKeyspaceFromCache()
public class PushPopSet {
    public final String NAME;

    private IgniteCache<String, SortedSet<String>> igniteCache = null;

    public PushPopSet(String name) {
        this.NAME = name;
    }

    public PushPopSet getOrCreate(Ignite ignite) {
        igniteCache = ignite.getOrCreateCache(NAME);
        return this;
    }

    public void putTx(IgniteTransactions t, String setName, String element) {
        try (Transaction tx = t.txStart()) {
            put(setName, element);
            tx.commit();
        }
    }

    public void put(String setName, String element) {
        put(setName, new TreeSet<>(Arrays.asList(element)));
    }

    public void put(String setName, Set<String> element) {
        SortedSet<String> updatedIndices = getKeyspaceFromCache(igniteCache, setName).orElse(new TreeSet<>());
        updatedIndices.addAll(element);
        igniteCache.put(setName, updatedIndices);
    }

    public String pop(IgniteTransactions t, String setName) {
        try (Transaction tx = t.txStart()) {
           Optional<SortedSet<String>> indicesOpt = getKeyspaceFromCache(igniteCache, setName);
           if (indicesOpt.isPresent()) {
               String pop = popAndGetFirstElement(indicesOpt.get());
               igniteCache.put(setName, indicesOpt.get());
               tx.commit();
               return pop;
           } else {
               throw new SetDoesNotExistException();
           }
        }
    }

    public Set<String> popAll(IgniteTransactions t, String setName) {
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


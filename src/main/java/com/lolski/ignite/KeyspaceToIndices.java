package com.lolski.ignite;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.transactions.Transaction;

import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

// TODO: differentiate between a mapping between
// a) a collection containing a keyspace with no indices to process, and
// b) a collection containing no keyspace
// Optional<?> getKeyspaceFromCache()
public class KeyspaceToIndices {
    public static final String KEYSPACE_TO_INDEX_CACHE_NAME = "grakn-queue-keyspace-to-index";

    private IgniteCache<String, SortedSet<String>> keyspaceToIndicesMap = null;

    public KeyspaceToIndices() {
    }

    public KeyspaceToIndices start(Ignite ignite) {
        keyspaceToIndicesMap = ignite.getOrCreateCache(KEYSPACE_TO_INDEX_CACHE_NAME);
        return this;
    }

    public void put(IgniteTransactions t, String keyspace, String index) {
        try (Transaction tx = t.txStart()) {
            SortedSet<String> updatedIndices = getKeyspaceFromCache(keyspaceToIndicesMap, keyspace).orElse(new TreeSet<>());
            updatedIndices.add(index);
            keyspaceToIndicesMap.put(keyspace, updatedIndices);
            tx.commit();
        }
    }

    public String pop(IgniteTransactions t, String keyspace) {
        try (Transaction tx = t.txStart()) {
           Optional<SortedSet<String>> indicesOpt = getKeyspaceFromCache(keyspaceToIndicesMap, keyspace);
           if (indicesOpt.isPresent()) {
               String pop = popAndGetFirstElement(indicesOpt.get());
               keyspaceToIndicesMap.put(keyspace, indicesOpt.get());
               tx.commit();
               return pop;
           } else {
               throw new KeyspaceDoesNotExistInKeyspaceToIndicesException();
           }
        }
    }

    public Set<String> getAll(String keyspace) {
        Set<String> all = keyspaceToIndicesMap.get(keyspace);
        return all;
    }

    private static Optional<SortedSet<String>> getKeyspaceFromCache(IgniteCache<String, SortedSet<String>> keyspaceToIndicesMap, String keyspace) {
        return Optional.ofNullable(keyspaceToIndicesMap.get(keyspace));
    }

    private String popAndGetFirstElement(SortedSet<String> indices) {
        String pop = indices.first();
        indices.remove(pop);
        return pop;
    }
}


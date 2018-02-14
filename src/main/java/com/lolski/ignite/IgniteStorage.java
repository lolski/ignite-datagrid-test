package com.lolski.ignite;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.Ignition;
import org.apache.ignite.transactions.Transaction;

import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

public class IgniteStorage implements AutoCloseable {
    private Ignite ignite = null;
    private KeyspaceToIndices keyspaceToIndicesMap = new KeyspaceToIndices();

    public IgniteStorage() {
    }

    public void start() {
        ignite = Ignition.start();
        keyspaceToIndicesMap.start(ignite);
    }

    @Override
    public void close() {
        ignite.close();
    }

    public void addIndex(String keyspace, String index, Set<String> conceptIds) {
        // TODO:
        // non atomic operations: keyspaceToIndicesMap.put followed by keyspaceAndIndex_ToConceptIdsMap.put
        // what is the implication?
        keyspaceToIndicesMap.put(ignite.transactions(), keyspace, index);
    }

    public String popIndex(String keyspace) {
        // TODO: check is getAndRemove() atomic?
        String toBePopped = keyspaceToIndicesMap.pop(ignite.transactions(), keyspace);
        return toBePopped;
    }

    public Set<String> popIds(String keyspace, String index) {
        throw new UnsupportedOperationException();
    }
}


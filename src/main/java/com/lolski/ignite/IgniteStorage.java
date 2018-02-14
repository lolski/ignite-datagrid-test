package com.lolski.ignite;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;

import java.util.Set;

public class IgniteStorage implements AutoCloseable {
    private Ignite ignite = null;
    private PushPopSortedSet pushPopSortedSetMap = new PushPopSortedSet("a-random-cache-name");

    public IgniteStorage() {
    }

    public void start() {
        ignite = Ignition.start();
        pushPopSortedSetMap.getOrCreate(ignite);
    }

    @Override
    public void close() {
        ignite.close();
    }

    public void addIndex(String keyspace, String index, Set<String> conceptIds) {
        // TODO:
        // non atomic operations: keyspaceToIndicesMap.put followed by keyspaceAndIndex_ToConceptIdsMap.put
        // what is the implication?
        pushPopSortedSetMap.put(ignite.transactions(), keyspace, index);
    }

    public String popIndex(String keyspace) {
        // TODO: check is getAndRemove() atomic?
        String toBePopped = pushPopSortedSetMap.pop(ignite.transactions(), keyspace);
        return toBePopped;
    }

    public Set<String> popIds(String keyspace, String index) {
        throw new UnsupportedOperationException();
    }
}


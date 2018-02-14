package com.lolski.ignite;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.transactions.Transaction;

import java.util.Set;

public class IgniteStorage implements AutoCloseable {
    private Ignite ignite = null;
    private PushPopSortedSet keyspaceToIndices = new PushPopSortedSet("grakn-keyspace-to-indices");
    private PushPopSortedSet keyspaceAndIndicesToConceptIds = new PushPopSortedSet("grakn-keyspace-and-indices-to-concept-ids");

    public IgniteStorage() {
    }

    public void start() {
        ignite = Ignition.start();
        keyspaceToIndices.getOrCreate(ignite);
    }

    @Override
    public void close() {
        ignite.close();
    }

    public void addIndex(String keyspace, String index, Set<String> conceptIds) {
        // TODO:
        // non atomic operations: keyspaceToIndicesMap.putTx followed by keyspaceAndIndex_ToConceptIdsMap.putTx
        // what is the implication?
        try (Transaction tx = ignite.transactions().txStart()) {
            keyspaceToIndices.put(keyspace, index);
            keyspaceAndIndicesToConceptIds.put(getConceptIdsKey(keyspace, index), conceptIds);
        }
    }

    public String popIndex(String keyspace) {
        // TODO: check is getAndRemove() atomic?
        String toBePopped = keyspaceToIndices.pop(ignite.transactions(), keyspace);
        return toBePopped;
    }

    public Set<String> popIds(String keyspace, String index) {
        Set<String> toBePopped = keyspaceAndIndicesToConceptIds.popAll(ignite.transactions(), getConceptIdsKey(keyspace, index));
        return toBePopped;
    }

    private static String getConceptIdsKey(String keyspace, String index){
        return "IdsToPostProcess_" + keyspace + "_Id_" + index;
    }
}


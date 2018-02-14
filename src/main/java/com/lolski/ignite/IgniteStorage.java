package com.lolski.ignite;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.transactions.Transaction;

import java.util.Set;

public class IgniteStorage implements AutoCloseable {
    private Ignite ignite = null;
    private IgniteMultiMap keyspaceToIndices = new IgniteMultiMap("grakn-keyspace-to-indices");
    private IgniteMultiMap keyspaceAndIndicesToConceptIds = new IgniteMultiMap("grakn-keyspace-and-indices-to-concept-ids");

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
        // non atomic operations: keyspaceToIndicesMap.putOneTx followed by keyspaceAndIndex_ToConceptIdsMap.putOneTx
        // what is the implication?
        try (Transaction tx = ignite.transactions().txStart()) {
            keyspaceToIndices.putOne(keyspace, index);
            keyspaceAndIndicesToConceptIds.putAll(getConceptIdsKey(keyspace, index), conceptIds);
            tx.commit();
        }
    }

    public String popIndex(String keyspace) {
        // TODO: check is getAndRemove() atomic?
        String toBePopped = keyspaceToIndices.popOne(ignite.transactions(), keyspace);
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


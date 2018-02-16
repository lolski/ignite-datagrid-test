package com.lolski.ignite;

import org.apache.ignite.Ignite;
import org.apache.ignite.transactions.Transaction;

import java.util.HashSet;
import java.util.Set;

public class IgniteIndexStorage implements AutoCloseable {
    private Ignite ignite;
    private IgniteMultiMap keyspaceToIndices;
    private IgniteMultiMap keyspaceAndIndicesToConceptIds;

    public IgniteIndexStorage(Ignite ignite, IgniteMultiMap keyspaceToIndices, IgniteMultiMap keyspaceAndIndicesToConceptIds) {
        this.ignite = ignite;
        this.keyspaceToIndices = keyspaceToIndices;
        this.keyspaceAndIndicesToConceptIds = keyspaceAndIndicesToConceptIds;
    }

    @Override
    public void close() {
        ignite.close();
    }

    public void addIndex(String keyspace, String index, Set<String> conceptIds) {
        try (Transaction tx = ignite.transactions().txStart()) {
            keyspaceToIndices.putOne(keyspace, index);
            keyspaceAndIndicesToConceptIds.putAll(getConceptIdsKey(keyspace, index), conceptIds);
            tx.commit();
        }
    }

    public String popIndex(String keyspace) {
        try {
            String toBePopped = keyspaceToIndices.popOneTx(ignite.transactions(), keyspace);
            return toBePopped;
        } catch (SetDoesNotExistException e) {
            return null;
        }
    }

    public Set<String> popIds(String keyspace, String index) {
        try {
            Set<String> toBePopped = keyspaceAndIndicesToConceptIds.popAllTx(ignite.transactions(), getConceptIdsKey(keyspace, index));
            return toBePopped;
        } catch (SetDoesNotExistException e) {
            return new HashSet<>();
        }
    }

    private static String getConceptIdsKey(String keyspace, String index){
        return "IdsToPostProcess_" + keyspace + "_Id_" + index;
    }
}


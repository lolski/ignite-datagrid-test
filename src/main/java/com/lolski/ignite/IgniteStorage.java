package com.lolski.ignite;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.transactions.Transaction;

import java.util.Set;
import java.util.SortedSet;

public class IgniteStorage implements AutoCloseable {
    private Ignite ignite = null;
    private IgniteMultiMap keyspaceToIndices = null;
    private IgniteMultiMap keyspaceAndIndicesToConceptIds = null;

    public IgniteStorage() {
    }

    public void start(DiscoverySettings discoverySettings, PersistenceSettings persistenceSettings) {
        ignite = IgniteFactory.createIgniteClusterMode(discoverySettings, persistenceSettings);
        keyspaceToIndices = new IgniteMultiMap(IgniteFactory.createIgniteCache(
                ignite, "a", 0, CacheMode.REPLICATED));
        keyspaceAndIndicesToConceptIds = new IgniteMultiMap(IgniteFactory.createIgniteCache(
                ignite, "b", 0, CacheMode.REPLICATED));
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
        String toBePopped = keyspaceToIndices.popOneTx(ignite.transactions(), keyspace);
        return toBePopped;
    }

    public Set<String> popIds(String keyspace, String index) {
        Set<String> toBePopped = keyspaceAndIndicesToConceptIds.popAllTx(ignite.transactions(), getConceptIdsKey(keyspace, index));
        return toBePopped;
    }

    private static String getConceptIdsKey(String keyspace, String index){
        return "IdsToPostProcess_" + keyspace + "_Id_" + index;
    }
}


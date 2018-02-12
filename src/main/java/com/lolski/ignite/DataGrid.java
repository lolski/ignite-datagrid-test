package com.lolski.ignite;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteQueue;
import org.apache.ignite.Ignition;
import org.jetbrains.annotations.Nullable;

import java.util.Set;

public class DataGrid implements AutoCloseable {
    public static final String KEYSPACE_TO_INDEX_CACHE_NAME = "grakn-queue-keyspace-to-index";
    public static final String KEYSPACE_AND_INDEX_TO_CONCEPT_IDS_CACHE_NAME = "grakn-queue-keyspace-and-index-to-concept-ids";
    public final String host;
    public final int port;

    private Ignite ignite = null;

    private IgniteCache<String, String> keyspaceToIndexMap = null;

    private IgniteCache<String, Set<String>> keyspaceAndIndex_ToConceptIdsMap = null;

    public DataGrid(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public void start() {
        ignite = Ignition.start();
        keyspaceToIndexMap = ignite.getOrCreateCache(KEYSPACE_TO_INDEX_CACHE_NAME);
        keyspaceAndIndex_ToConceptIdsMap = ignite.getOrCreateCache(KEYSPACE_AND_INDEX_TO_CONCEPT_IDS_CACHE_NAME);
    }

    @Override
    public void close() {
        ignite.close();
    }

    public void addIndex(String keyspace, String index, Set<String> conceptIds) {
        // TODO:
        // non atomic operations: keyspaceToIndexMap.put followed by keyspaceAndIndex_ToConceptIdsMap.put
        // what is the implication?
        keyspaceToIndexMap.put(keyspace, index);
        keyspaceAndIndex_ToConceptIdsMap.put(getConceptIdsKey(keyspace, index), conceptIds);
    }

    @Nullable
    public String popIndex(String keyspace) {
        // TODO: check is getAndRemove() atomic?
        String toBePopped = keyspaceToIndexMap.getAndRemove(keyspace);
        return toBePopped;
    }

    public Set<String> popIds(String keyspace, String index) {
        Set<String> conceptIds = keyspaceAndIndex_ToConceptIdsMap.getAndRemove(getConceptIdsKey(keyspace, index));
        return conceptIds;
    }

    static String getConceptIdsKey(String keyspace, String index){
        return "IdsToPostProcess_" + keyspace + "_Id_" + index;
    }
}

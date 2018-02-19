package com.lolski.ignite;

import org.apache.ignite.Ignite;

public class IgniteCountStorage {
    private Ignite ignite;
    private IgniteMapOfLong numOfInstancesPerConceptId;
    private IgniteMapOfLong numOfShardsPerConceptId;

    public IgniteCountStorage(Ignite ignite, IgniteMapOfLong numOfInstancesPerConceptId,
                              IgniteMapOfLong numOfShardsPerCOnceptId) {
        this.ignite = ignite;
        this.numOfInstancesPerConceptId = numOfInstancesPerConceptId;
        this.numOfShardsPerConceptId = numOfShardsPerCOnceptId;
    }

    public long adjustNumOfInstancesCount(String key, long count) {
        return numOfInstancesPerConceptId.incrementByTx(ignite.transactions(), key, count);
    }

    public long adjustNumOfShardsCount(String key, long count) {
        return numOfShardsPerConceptId.incrementByTx(ignite.transactions(), key, count);
    }

    public long getInstanceCount(String key) {
        return numOfInstancesPerConceptId.get(key);
    }

    public long getShardCount(String key) {
        return numOfShardsPerConceptId.get(key);
    }
}


package com.lolski.ignite;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

public class Main {
    public static void main(String[] args) {
        int port = Integer.parseInt(args[0]);
        String discovery[] = Arrays.copyOfRange(args, 1, args.length);
        String localAddress = getLocalNodeAndSeeds(discovery).getKey();
        String[] seeds = getLocalNodeAndSeeds(discovery).getValue().toArray(new String[] {});
        Path storagePath = attemptCreateDir(Paths.get("./db/ignite"));

        Ignite cluster = IgniteFactory.createIgniteClusterMode(
                new DiscoverySettings(localAddress, seeds), new PersistenceSettings(storagePath));
        IgniteCache<String, SortedSet<String>> cache =
                IgniteFactory.createIgniteCache(cluster,"test", 0, CacheMode.REPLICATED);

        IgniteMultiMap map = new IgniteMultiMap(cache);

        RestEndpoints.setupRestEndpoints(port,
                () -> map.getKeys().stream().collect(Collectors.joining(", ")),
                value -> map.putOneTx(cluster.transactions(), localAddress, value),
                key -> map.getAll(key).stream().collect(Collectors.joining(", ")));
    }

    private static Map.Entry<String, List<String>> getLocalNodeAndSeeds(String[] args) {
        switch (args.length) {
            case 0:
                return new HashMap.SimpleEntry<>("localhost", Arrays.asList());
            case 1:
                return new HashMap.SimpleEntry<>(args[0], Arrays.asList());
            default:
                return new HashMap.SimpleEntry<>(args[0], Arrays.asList(Arrays.copyOfRange(args, 1, args.length)));
        }
    }

    private static Path attemptCreateDir(Path dir) {
        if (!dir.toFile().exists()) {
            boolean attemptCreateDir = dir.toFile().mkdirs();
            if (!attemptCreateDir) {
                throw new RuntimeException("Unable to create directory " + dir.toAbsolutePath().toString());
            }
        }
        return dir;
    }
}
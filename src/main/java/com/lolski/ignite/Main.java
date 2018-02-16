package com.lolski.ignite;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Main {
    public static void main(String[] args) {
        String localAddress = getLocalNodeAndSeeds(args).getKey();
        String[] seeds = getLocalNodeAndSeeds(args).getValue().toArray(new String[] {});
        Path storagePath = attemptCreateDir(Paths.get("./db/ignite"));

        IgniteCluster cluster = new IgniteCluster(new DiscoverySettings(localAddress, seeds), new PersistenceSettings(storagePath)).start();
        IgniteMultiMap map = new IgniteMultiMap("test").getOrCreate(cluster.ignite);
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


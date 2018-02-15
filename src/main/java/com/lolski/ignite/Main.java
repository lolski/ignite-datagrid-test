package com.lolski.ignite;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Main {
    public static void main(String[] args) {
        String localAddress = getLocalNodeAndSeeds(args).getKey();
        String[] seeds = getLocalNodeAndSeeds(args).getValue().toArray(new String[] {});

        IgniteCluster cluster = new IgniteCluster(localAddress, seeds).start();
        System.out.println("====");
        cluster.ignite.cluster().localNode().addresses().forEach(System.out::println);
        System.out.println("====");
//        IgniteMultiMap map = new IgniteMultiMap("test").getOrCreate(cluster.ignite);
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
}


package com.lolski.ignite;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Main {
    public static void main(String[] args) {
//        assertArgumentCorrect(args);
//
//        // TODO: use host port
//        String host = args[0];
//        int port = Integer.parseInt(args[1]);
        String localAddress = getLocalNodeAndSeeds(args).getKey();
        String[] seeds = getLocalNodeAndSeeds(args).getValue().toArray(new String[] {});

        IgniteCluster cluster = new IgniteCluster(localAddress, seeds).start();
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

    private static void assertArgumentCorrect(String[] args) {
        String help = "Usage: <program> <host> <port> (port must be a number)";
        if (args.length != 2) {
            System.err.println(help);
            System.exit(1);
        }

        try {
            Integer.parseInt(args[1]);
        } catch (NumberFormatException e) {
            System.err.println(help);
            System.exit(1);
        }
    }
}


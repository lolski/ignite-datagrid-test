package com.lolski.ignite;

public class Main {
    public static void main(String[] args) {
        assertArgumentCorrect(args);

        // TODO: use host port
        String host = args[1];
        int port = Integer.parseInt(args[2]);
        IgniteCluster cluster = new IgniteCluster(port).start();
        IgniteMultiMap map = new IgniteMultiMap("test").getOrCreate(cluster.ignite);
    }

    private static void assertArgumentCorrect(String[] args) {
        String help = "Usage: <program> <host> <port> (port must be a number)";
        if (args.length != 2) {
            System.err.println(help);
            System.exit(1);
        }

        try {
            Integer.parseInt(args[2]);
        } catch (NumberFormatException e) {
            System.err.println(help);
            System.exit(1);
        }
    }
}


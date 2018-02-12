package com.lolski.ignite;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;

import java.util.Set;

public class Main {
    public static void main(String[] args) {
        assertArgumentCorrect(args);

        String host = args[1];
        int port = Integer.parseInt(args[2]);

        DataGrid dataGrid = new DataGrid(host, port);
    }

    private static void assertArgumentCorrect(String[] args) {
        String help = "Usage: <program> <host> <port> (port must be a number)";
        if (args.length != 3) {
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


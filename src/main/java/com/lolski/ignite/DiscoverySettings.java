package com.lolski.ignite;

public class DiscoverySettings {
    public final String localAddress;
    public final String[] seeds;

    public DiscoverySettings(String localAddress, String... seeds) {
        this.localAddress = localAddress;
        this.seeds = seeds;
    }
}

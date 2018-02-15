package com.lolski.ignite;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

import java.util.Arrays;

public class IgniteCluster {
    public final int PORT;
    public IgniteConfiguration igniteConfiguration;
    public Ignite ignite;

    public IgniteCluster(int port) {
        this.PORT = port;
        this.igniteConfiguration = new IgniteConfiguration().setDiscoverySpi(getTcpDiscoverySpi("localhost:"+PORT));
    }

    public IgniteCluster start() {
        ignite = Ignition.start(igniteConfiguration);
        return this;
    }

    private TcpDiscoverySpi getTcpDiscoverySpi(String... addresses) {
        TcpDiscoverySpi spi = new TcpDiscoverySpi();
        TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();
        ipFinder.setAddresses(Arrays.asList(addresses));
        spi.setIpFinder(ipFinder);
        return spi;
    }
}

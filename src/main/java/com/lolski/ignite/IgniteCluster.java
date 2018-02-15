package com.lolski.ignite;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

import java.util.Arrays;

public class IgniteCluster {
//    public final int PORT;
    public IgniteConfiguration igniteConfiguration;
    public Ignite ignite;

    public IgniteCluster(String localAddress, String... seeds) {
//        this.PORT = port;
        this.igniteConfiguration = new IgniteConfiguration().setDiscoverySpi(getTcpDiscoverySpi(localAddress, seeds));
    }

    public IgniteCluster start() {
        ignite = Ignition.start(igniteConfiguration);
        return this;
    }

    private TcpDiscoverySpi getTcpDiscoverySpi(String localAddress, String... seeds) {
        TcpDiscoverySpi spi = new TcpDiscoverySpi();
        TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();
        ipFinder.setAddresses(Arrays.asList(seeds));
        spi.setLocalAddress(localAddress);
        spi.setIpFinder(ipFinder);
        return spi;
    }
}

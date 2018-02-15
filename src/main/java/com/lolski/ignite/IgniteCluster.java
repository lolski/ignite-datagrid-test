package com.lolski.ignite;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

import java.util.Arrays;

public class IgniteCluster {
//    public final int PORT;
    public IgniteConfiguration igniteConfiguration;
    public Ignite ignite;

    public IgniteCluster(String localAddress, String... seeds) {
//        this.PORT = port;
        this.igniteConfiguration = new IgniteConfiguration()
                .setCommunicationSpi(getTcpCommunicationSpi(localAddress))
                .setDiscoverySpi(getTcpDiscoverySpi(seeds));

// listen to localhost:5555
Ignition.start(new IgniteConfiguration()
    .setCommunicationSpi(new TcpCommunicationSpi().setLocalAddress("localhost").setLocalPort(5555))
    .setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(new TcpDiscoveryVmIpFinder().setShared(true).setAddresses(Arrays.asList()))));

// listen to localhost:5556, form a cluster with localhost:5555
Ignition.start(new IgniteConfiguration()
    .setCommunicationSpi(new TcpCommunicationSpi().setLocalAddress("localhost").setLocalPort(5556))
    .setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(new TcpDiscoveryVmIpFinder().setShared(true).setAddresses(Arrays.asList("localhost:5555")))));
    }

    public IgniteCluster start() {
        ignite = Ignition.start(igniteConfiguration);
        return this;
    }

    private TcpCommunicationSpi getTcpCommunicationSpi(String localAddress) {
        String[] hostAndPort = localAddress.split(":");
        return new TcpCommunicationSpi()
                .setLocalAddress(hostAndPort[0])
                .setLocalPort(Integer.parseInt(hostAndPort[1]));
    }

    private TcpDiscoverySpi getTcpDiscoverySpi(String... seeds) {
        TcpDiscoveryVmIpFinder ipFinder =
                new TcpDiscoveryVmIpFinder()
                .setShared(true)
                .setAddresses(Arrays.asList(seeds));
        TcpDiscoverySpi spi = new TcpDiscoverySpi()
            .setIpFinder(ipFinder);
        return spi;
    }
}

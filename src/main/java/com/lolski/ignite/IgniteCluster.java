package com.lolski.ignite;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
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
        this.igniteConfiguration = getIgniteConfiguration(getTcpCommunicationSpi(localAddress),
                getTcpDiscoverySpi(seeds), getDurableDataStorageConfiguration());
    }

    public IgniteCluster start() {
        ignite = Ignition.start(igniteConfiguration);
        return this;
    }

    public static IgniteConfiguration getIgniteConfiguration(TcpCommunicationSpi tcpCommunicationSpi, TcpDiscoverySpi tcpDiscoverySpi, DataStorageConfiguration dataStorageConfiguration) {
        return new IgniteConfiguration()
                .setCommunicationSpi(tcpCommunicationSpi)
                .setDiscoverySpi(tcpDiscoverySpi)
                .setDataStorageConfiguration(dataStorageConfiguration);
    }

    public static DataStorageConfiguration getDurableDataStorageConfiguration() {
        DataStorageConfiguration dataStorageConfiguration = new DataStorageConfiguration();
        DataRegionConfiguration dataRegionConfiguration = dataStorageConfiguration
                .getDefaultDataRegionConfiguration().setPersistenceEnabled(true);

        return dataStorageConfiguration.setDefaultDataRegionConfiguration(dataRegionConfiguration);
    }

    public static TcpCommunicationSpi getTcpCommunicationSpi(String localAddress) {
        String[] hostAndPort = localAddress.split(":");
        return new TcpCommunicationSpi()
                .setLocalAddress(hostAndPort[0])
                .setLocalPort(Integer.parseInt(hostAndPort[1]));
    }

    public static TcpDiscoverySpi getTcpDiscoverySpi(String... seeds) {
        TcpDiscoveryVmIpFinder ipFinder =
                new TcpDiscoveryVmIpFinder()
                .setShared(true)
                .setAddresses(Arrays.asList(seeds));
        TcpDiscoverySpi spi = new TcpDiscoverySpi()
            .setIpFinder(ipFinder);
        return spi;
    }
}


//// listen to localhost:5555
//Ignition.start(new IgniteConfiguration()
//        .setCommunicationSpi(new TcpCommunicationSpi().setLocalAddress("localhost").setLocalPort(5555))
//        .setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(new TcpDiscoveryVmIpFinder().setShared(true).setAddresses(Arrays.asList()))));
//
//// listen to localhost:5556, form a cluster with localhost:5555
//        Ignition.start(new IgniteConfiguration()
//        .setCommunicationSpi(new TcpCommunicationSpi().setLocalAddress("localhost").setLocalPort(5556))
//        .setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(new TcpDiscoveryVmIpFinder().setShared(true).setAddresses(Arrays.asList("localhost:5555")))));
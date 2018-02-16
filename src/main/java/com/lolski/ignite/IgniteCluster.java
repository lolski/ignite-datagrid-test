package com.lolski.ignite;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

import java.nio.file.Path;
import java.util.Arrays;

public class IgniteCluster {
    public IgniteConfiguration igniteConfiguration;
    public Ignite ignite;

    public IgniteCluster(DiscoverySettings discoverySettings, PersistenceSettings persistenceSettings) {
        this.igniteConfiguration = getIgniteConfiguration(getTcpCommunicationSpi(discoverySettings.localAddress),
                getTcpDiscoverySpi(discoverySettings.localAddress, discoverySettings.seeds),
                getDurableDataStorageConfiguration(persistenceSettings.location));
    }

    public IgniteCluster start() {
        ignite = Ignition.start(igniteConfiguration);
        ignite.active(true);
        return this;
    }

    public static IgniteConfiguration getIgniteConfiguration(TcpCommunicationSpi tcpCommunicationSpi, TcpDiscoverySpi tcpDiscoverySpi, DataStorageConfiguration dataStorageConfiguration) {
        return new IgniteConfiguration()
                .setCommunicationSpi(tcpCommunicationSpi)
                .setDiscoverySpi(tcpDiscoverySpi)
                .setDataStorageConfiguration(dataStorageConfiguration);
    }

    public static DataStorageConfiguration getDurableDataStorageConfiguration(Path location) {
        DataStorageConfiguration dataStorageConfiguration = new DataStorageConfiguration()
                .setStoragePath(location.toAbsolutePath().toString());
        DataRegionConfiguration dataRegionConfiguration = dataStorageConfiguration
                .getDefaultDataRegionConfiguration().setPersistenceEnabled(true);

        return dataStorageConfiguration.setDefaultDataRegionConfiguration(dataRegionConfiguration);
    }

    public static TcpCommunicationSpi getTcpCommunicationSpi(String localAddress) {
        return new TcpCommunicationSpi();
    }

    public static TcpDiscoverySpi getTcpDiscoverySpi(String localAddress, String... seeds) {
        TcpDiscoveryVmIpFinder ipFinder =
                new TcpDiscoveryVmIpFinder()
                .setShared(true)
                .setAddresses(Arrays.asList(seeds));

        String[] hostAndPort = localAddress.split(":");
        TcpDiscoverySpi spi = new TcpDiscoverySpi()
                .setLocalAddress(hostAndPort[0])
                .setLocalPort(Integer.parseInt(hostAndPort[1]))
                .setIpFinder(ipFinder);

        return spi;
    }
}


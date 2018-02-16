package com.lolski.ignite;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

import java.nio.file.Path;
import java.util.Arrays;

public class IgniteFactory {
    public static Ignite createIgniteClusterMode(DiscoverySettings discoverySettings, PersistenceSettings persistenceSettings) {
        IgniteConfiguration igniteConfiguration = getIgniteConfiguration(getTcpCommunicationSpi(discoverySettings.localAddress),
                getTcpDiscoverySpi(discoverySettings.localAddress, discoverySettings.seeds),
                getDurableDataStorageConfiguration(persistenceSettings.location));
        Ignite ignite = Ignition.start(igniteConfiguration);
        ignite.active(true);
        return ignite;
    }

    public static <K, V> IgniteCache<K, V> createIgniteCache(Ignite ignite, String name, int backup, CacheMode cacheMode) {
        return ignite.getOrCreateCache(getCacheConfiguration(name, backup, cacheMode));
    }

    private static <K, V> CacheConfiguration<K, V> getCacheConfiguration(String name, int backup, CacheMode cacheMode) {
        return new CacheConfiguration<K, V>()
                .setName(name)
                .setBackups(backup)
                .setCacheMode(cacheMode);
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


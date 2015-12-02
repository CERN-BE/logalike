/**
 * Logalike - A stream based message processor
 * Copyright (c) 2015 European Organisation for Nuclear Research (CERN), All Rights Reserved.
 * This software is distributed under the terms of the GNU General Public Licence version 3 (GPL Version 3),
 * copied verbatim in the file “COPYLEFT”.
 * In applying this licence, CERN does not waive the privileges and immunities granted to it by virtue
 * of its status as an Intergovernmental Organization or submit itself to any jurisdiction. 
 * 
 * Authors: Gergő Horányi <ghoranyi> and Jens Egholm Pedersen <jegp>
 */

package cern.acet.tracing.output.elasticsearch;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.stream.Collectors;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.Settings.Builder;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A client builder for Elasticsearch that can build clients with different settings and spawn nodes that is a
 * connection to an elasticsearch cluster. The spawned nodes are seen purely as a client node that does not contain any
 * documents (contrary to a data node).
 *
 * @author jepeders
 */
public class ClientBuilder {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClientBuilder.class);

    private static final String UNICAST_HOSTS_KEY = "discovery.zen.ping.unicast.hosts";
    private static final String ES_HOME = System.getProperty("user.dir");
    private final NodeBuilder nodeBuilder;
    private final Builder settingsBuilder;

    /**
     * Creates a {@link ClientBuilder} with default settings for a client node.
     */
    public ClientBuilder() {
        this(getDefaultSettings());
    }

    /**
     * Created a {@link ClientBuilder} that uses the given settings.
     *
     * @param settings The {@link Settings} to use when building a {@link Node}.
     */
    public ClientBuilder(Settings settings) {
        this.settingsBuilder = Settings.settingsBuilder().put(settings);
        this.nodeBuilder = NodeBuilder.nodeBuilder();
    }

    private ClientBuilder(Builder settingsBuilder) {
        this.settingsBuilder = settingsBuilder;
        this.nodeBuilder = NodeBuilder.nodeBuilder();
    }

    /**
     * Creates a {@link ClientBuilder} with the given settings applied and that uses the given {@link NodeBuilder} when
     * constructing a {@link Node}.
     *
     * @param settingsBuilder The settings to apply at class construction.
     * @param nodeBuilder The {@link NodeBuilder} to use when constructing the {@link Node}.
     */
    ClientBuilder(Builder settingsBuilder, NodeBuilder nodeBuilder) {
        this.nodeBuilder = nodeBuilder;
        this.settingsBuilder = settingsBuilder;
    }

    /**
     * Sets the name of the cluster to use when discovering the cluster with multicast.
     *
     * @param clusterName The name of the cluster to discover.
     * @return The same {@link ClientBuilder} with the 'cluster.name' property set.
     */
    public ClientBuilder setClusterName(String clusterName) {
        settingsBuilder.put("cluster.name", clusterName);
        return this;
    }

    /**
     * Sets the host of the cluster to use when discovering the cluster with unicast.
     *
     * @param hosts The hosts to search for when connecting to the cluster.
     * @return The same {@link ClientBuilder} with the 'discovery.zen.ping.unicast.hosts' property set. If the hosts
     *         list is empty, we return the same builder.
     */
    public ClientBuilder setHosts(List<String> hosts) {
        if (hosts.isEmpty()) {
            return this;
        } else {
            settingsBuilder.put(UNICAST_HOSTS_KEY, hosts.stream().collect(Collectors.joining(",")));
            return this;
        }
    }

    /**
     * Sets the name of the node as it appears in the Elasticsearch cluster.
     *
     * @param name The name of the node.
     * @return The same {@link ClientBuilder} with the 'node.name' property set.
     */
    public ClientBuilder setNodeName(String name) {
        settingsBuilder.put("node.name", name);
        return this;
    }

    /**
     * Creates a client connection from the settings given in the constructor.
     *
     * @return A {@link Client} if a connection was successfully made.
     * @throws ElasticsearchException If the connection to the cluster failed.
     */
    public Client build() {
        if (settingsBuilder.get(UNICAST_HOSTS_KEY) == null) {
            throw new IllegalArgumentException("No hosts defined; Cannot create a client with no hosts to connect to.");
        }
        return nodeBuilder.settings(settingsBuilder).node().client();
    }

    /**
     * Creates a number of default settings that prepares a client connection to an Elasticsearch cluster.
     *
     * @return A {@link Builder} object to be used for the cluster connection.
     */
    static Builder getDefaultSettings() {
        String hostname;
        try {
            hostname = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            hostname = "localhost";
        }
        LOGGER.info("Set host to {}", hostname);

        //@formatter:off
        return Settings.settingsBuilder()
                .put("path.home", ES_HOME)
                .put("node.master", false)
                .put("transport.host", hostname)
                .put("node.client", true) /* Set to client mode so this node will hold no data */
                .put("http.enabled", false) /* Disable http requests on this node */
                .put("client.transport.sniff", true); /* Sniff the rest of the cluster for redundancy */
        //@formatter:on
    }
}
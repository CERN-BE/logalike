/**
 * Logalike - A stream based message processor Copyright (c) 2015 European Organisation for Nuclear Research (CERN), All
 * Rights Reserved. This software is distributed under the terms of the GNU General Public Licence version 3 (GPL
 * Version 3), copied verbatim in the file “COPYLEFT”. In applying this licence, CERN does not waive the privileges and
 * immunities granted to it by virtue of its status as an Intergovernmental Organization or submit itself to any
 * jurisdiction. Authors: Gergő Horányi <ghoranyi> and Jens Egholm Pedersen <jegp>
 */

package cern.acet.tracing.output;

import java.io.Closeable;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import cern.acet.tracing.CloseableOutput;
import org.elasticsearch.client.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cern.acet.tracing.Input;
import cern.acet.tracing.Message;
import cern.acet.tracing.Output;
import cern.acet.tracing.output.elasticsearch.BulkConsumer;
import cern.acet.tracing.output.elasticsearch.ClientBuilder;
import cern.acet.tracing.output.elasticsearch.ElasticsearchIndex;
import cern.acet.tracing.output.elasticsearch.ElasticsearchMessage;
import cern.acet.tracing.output.elasticsearch.ElasticsearchTemplateMapping;
import cern.acet.tracing.processing.Processor;
import cern.acet.tracing.util.CloseableConsumer;
import cern.acet.tracing.util.type.TypeStrategy;
import cern.acet.tracing.util.type.strategy.AcceptStrategy;

import com.google.common.collect.ImmutableMap;

/**
 * An output that serves data into an Elasticsearch cluster. Supports Elasticsearch >= 2.0.
 *
 * @author ghoranyi, jepeders
 */
public class ElasticsearchOutput implements CloseableOutput<ElasticsearchMessage> {

    private static final String CONSTRUCTOR_FORMAT_STRING = "Created ElasticsearchOutput with type strategy {} "
            + "and type mapping {}";
    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticsearchOutput.class);
    private static final ElasticsearchIndex DEFAULT_LOGALIKE_INDEX = ElasticsearchIndex.daily("logalike");

    /**
     * Interval of time in minutes when the bulk will be flushed to Elasticsearch.
     */
    public static final Duration FLUSH_INTERVAL_MINUTES = Duration.ofMinutes(1);

    private final BulkConsumer consumer;
    private final Optional<ElasticsearchTemplateMapping> mappingOption;
    private final TypeStrategy typeStrategy;
    private final Client client;

    /**
     * Constructs an {@link ElasticsearchOutput} that sends messages to a cluster connection, built by the set
     * parameters in the builder.
     *
     * @param builder A {@link Builder} that contains the necessary information to connect to an Elasticsearch cluster.
     */
    ElasticsearchOutput(Builder builder) {
        this.client = builder.getClient();
        this.consumer = builder.getConsumer(client);
        this.mappingOption = builder.mapping;
        this.typeStrategy = builder.typeStrategy;
        LOGGER.info(CONSTRUCTOR_FORMAT_STRING, typeStrategy, mappingOption.map(mapping -> mapping.getTypeMap(client))
                .orElse(ImmutableMap.of()));
    }

    @Override
    public void accept(ElasticsearchMessage message) {
        consumer.accept(message);
    }

    /**
     * @return A {@link Builder} instance to build an {@link ElasticsearchOutput}.
     */
    public static Builder builder() {
        return new Builder();
    }

    @Override
    public void close() {
        try {
            consumer.close();
        } catch (Exception e) {
            LOGGER.warn("Error when closing Elasticsearch connection", e);
        }
    }

    /**
     * Creates an {@link ElasticsearchMessage} which includes the key-value type constraints as defined by this
     * {@link ElasticsearchOutput}.
     *
     * @return An {@link ElasticsearchMessage}.
     */
    public ElasticsearchMessage createTypedMessage() {
        return mappingOption.map(mapping -> ElasticsearchMessage.of(mapping.getTypeMap(client), typeStrategy)).orElse(
                ElasticsearchMessage.of(typeStrategy));
    }

    /**
     * Flushes all pending messages to Elasticsearch.
     */
    public void flush() {
        consumer.flush();
    }

    /**
     * A builder for an {@link ElasticsearchOutput}.
     *
     * @author jepeders
     */
    public static class Builder {

        private String clusterName = "elasticsearch";
        private ElasticsearchIndex defaultIndex = DEFAULT_LOGALIKE_INDEX;
        private List<String> hosts = new ArrayList<>();
        private Optional<ElasticsearchTemplateMapping> mapping = Optional.empty();
        private TypeStrategy typeStrategy = AcceptStrategy.INSTANCE;
        private Duration flushInterval = FLUSH_INTERVAL_MINUTES;
        private String documentType = "logalike";

        private String nodeName;

        /**
         * Creates a {@link Builder} instance that can construct {@link BulkConsumer}s.
         */
        public Builder() {
            String hostName;
            try {
                hostName = InetAddress.getLocalHost().getHostName();
            } catch (UnknownHostException e) {
                hostName = "unknown host";
            }

            try {
                final String pid = ManagementFactory.getRuntimeMXBean().getName().split("@")[0];
                this.nodeName = "Logsmart-" + pid + "@" + hostName;
            } catch (Exception exception) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Failed to get pid for process. Resorting to Java hashcode instead");
                }
                this.nodeName = "Logsmart-" + hashCode() + "@" + hostName;
            }
        }

        /**
         * Adds the given host to the Elasticsearch cluster to connect to.
         *
         * @param host A host to connect to when the {@link ElasticsearchOutput} is started.
         * @return The same builder with the host set.
         */
        public Builder addHost(String host) {
            this.hosts.add(host);
            return this;
        }

        /**
         * Gets a {@link CloseableConsumer} that consumes messages and inserts them into the Elasticsearch cluster, via
         * the given client.
         *
         * @param client The {@link Client} to use as transport to an Elasticsearch cluster.
         * @return A {@link CloseableConsumer}.
         */
        public BulkConsumer getConsumer(Client client) {
            return new BulkConsumer(client, defaultIndex, flushInterval, documentType);
        }

        /**
         * @return An instance of an {@link ElasticsearchOutput}.
         */
        public ElasticsearchOutput build() {
            return new ElasticsearchOutput(this);
        }

        Client getClient() {
            //@formatter:off
            return new ClientBuilder()
                .setClusterName(clusterName)
                .setHosts(hosts)
                .setNodeName(nodeName)
                .build();
            //@formatter:on
        }

        /**
         * Sets the name of the Elasticsearch cluster to connect to. Used to locate clusters using TCP multicast. This
         * is a required parameter, but can be supplemented by {@link #addHost(String)} to locate individual hosts
         * outside the multicast network.
         *
         * @param clusterName The name of the cluster.
         * @return The same builder with the cluster name set.
         */
        public Builder setClusterName(String clusterName) {
            this.clusterName = clusterName;
            return this;
        }

        /**
         * Sets the default {@link ElasticsearchIndex} where messages are stored to per default. This value is
         * overwritten if at least one index have been assigned in a {@link Message} by either the {@link Input},
         * {@link Processor} or {@link Output}.
         *
         * @param defaultIndex The {@link ElasticsearchIndex} to store messages to per default.
         * @return The same builder with the default index set.
         */
        public Builder setDefaultIndex(ElasticsearchIndex defaultIndex) {
            this.defaultIndex = defaultIndex;
            return this;
        }

        /**
         * Sets the name of the document type which messages in this output will be inserted under. Defaults to
         * "logalike".
         *
         * @param documentType The name of the document type defined in the Elasticsearch cluster.
         * @return The same builder with the name of the document type set.
         */
        public Builder setDocumentType(String documentType) {
            this.documentType = documentType;
            return this;
        }

        /**
         * Sets the interval with which messages are flushed to the Elasticsearch cluster. If no mesages have been
         * queued, nothing will happen.
         *
         * @param flushInterval The interval with with messages should be flushed.
         * @return The same builder with the flush interval set.
         */
        public Builder setFlushInterval(Duration flushInterval) {
            if (flushInterval.isNegative() || flushInterval.isZero()) {
                throw new IllegalArgumentException("Flush interval cannot be negative or zero: " + flushInterval);
            }
            this.flushInterval = flushInterval;
            return this;
        }

        /**
         * The name of the input as it will appear in Elasticsearch. If this is not set, the name will appear as
         * 'logalike' and its pid, if found.
         *
         * @param nodeName The name of the input.
         * @return The same builder with the input name set.
         */
        public Builder setNodeName(String nodeName) {
            this.nodeName = nodeName;
            return this;
        }

        /**
         * Sets the Elasticsearch type mapping of the output. If this is not set, the type mapping will be empty, and no
         * type restraints will be put on the messages.
         *
         * @param mapping An instance of a {@link ElasticsearchTemplateMapping}.
         * @return The same builder with the type mapping set.
         */
        public Builder setMapping(ElasticsearchTemplateMapping mapping) {
            this.mapping = Optional.of(mapping);
            return this;
        }

        /**
         * Defines the behaviour when values are stored under keys, that does not have any type defined.
         *
         * @param typeStrategy A strategy for when keys have no types
         * @return This builder with the type strategy defined.
         * @see TypeStrategy
         */
        public Builder setTypeStrategy(TypeStrategy typeStrategy) {
            this.typeStrategy = typeStrategy;
            return this;
        }

    }

}

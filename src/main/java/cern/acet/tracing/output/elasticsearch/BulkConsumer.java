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

import java.io.IOException;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cern.acet.tracing.util.CloseableConsumer;
import cern.acet.tracing.util.type.TypeConstraint;

/**
 * Handles index requests for messages using a {@link BulkProcessor} that flushes every minute or if size exceeds 5Mb.
 *
 * @author jepeders
 */
public class BulkConsumer implements CloseableConsumer<ElasticsearchMessage> {

    /**
     * This formatter uses the pattern <code>'YYYY-MM-dd'T'HH:mm:ss.SSSZ'</code> which enforces milliseconds. Timestamps
     * without them are not properly parsed in Elasticsearch.
     */
    public static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter
            .ofPattern("YYYY-MM-dd'T'HH:mm:ss.SSSZ");

    private static final int DEFAULT_CONCURRENT_REQUESTS = 4;
    private static final int BULK_ACTIONS_LIMIT = 1000;

    private static final Logger LOGGER = LoggerFactory.getLogger(BulkConsumer.class);

    private final ElasticsearchIndex defaultIndex;
    private final String documentType;
    private final BulkProcessor processor;

    /**
     * Creates a bulk manager using the given {@link Client} and the default index for messages without any
     * index/indices assigned.
     *
     * @param client The client to use as target for the messages.
     * @param defaultIndex The default index used if any message does not have an index assigned.
     * @param flushInterval The interval with which bulks should be flushed if they are not empty.
     * @param documentType The name of the document type to store the messages under.
     */
    public BulkConsumer(Client client, ElasticsearchIndex defaultIndex, Duration flushInterval, String documentType) {
        this(getProcessorFromManager(client, flushInterval), defaultIndex, documentType);
    }

    /**
     * Creates a bulk manager using the given {@link BulkProcessorManager} and the default index for messages without
     * any index/indices assigned.
     *
     * @param processor The {@link BulkProcessor} that can dispatch messages in bulks.
     * @param defaultIndex The default index used if any message does not have an index assigned.
     * @param documentType The name of the document type to store the messages under.
     */
    BulkConsumer(BulkProcessor processor, ElasticsearchIndex defaultIndex, String documentType) {
        this.defaultIndex = defaultIndex;
        this.processor = processor;
        this.documentType = documentType;
    }

    @Override
    public void accept(ElasticsearchMessage message) {
        try {
            List<ElasticsearchIndex> indices = message.getIndices();
            if (indices.isEmpty()) {
                if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace(String.format("No index found for message %s. Storing under default index %s",
                            message, defaultIndex.toString()));
                }
                storeMessageToIndex(message, defaultIndex);
            } else {
                indices.stream().forEach(index -> storeMessageToIndex(message, index));
            }
        } catch (ConcurrentModificationException e) {
            LOGGER.error("Error storing message", e);
        }
    }

    /**
     * Flushes the current bulk by submitting all pending requests.
     */
    public void flush() {
        processor.flush();
    }

    /**
     * Creates a {@link BulkProcessor} using the given {@link Client}.
     *
     * @param client The client to use when constructing the {@link BulkProcessor}.
     * @param flushInterval The interval with which bulks will be flushed.
     * @return A {@link BulkProcessor} to bulk-process messages.
     */
    private static BulkProcessor getProcessorFromManager(Client client, Duration flushInterval) {
        if (flushInterval.isNegative() || flushInterval.isZero()) {
            throw new IllegalArgumentException("Flush interval cannot be zero or below: " + flushInterval);
        }

        BulkProcessor.Builder builder = BulkProcessor.builder(client, new BulkProcessorListener());
        builder.setBulkActions(BULK_ACTIONS_LIMIT);
        builder.setConcurrentRequests(DEFAULT_CONCURRENT_REQUESTS);
        builder.setFlushInterval(TimeValue.timeValueMillis(flushInterval.toMillis()));
        return builder.build();
    }

    /**
     * Adds an index request to store a single message to a single index.
     *
     * @param message The message to index.
     * @param index The index of the message.
     * @param metrics The metrics to record the storage in.
     */
    private void storeMessageToIndex(ElasticsearchMessage message, ElasticsearchIndex index) {
        /*
         * Implementation note: Elasticsearch is not happy about the ZonedDateTime toString format, so we need to format
         * the timestamp using a time zone offset instead.
         */
        Map<String, Object> sourceMap = message.toMap();
        Map<String, String> formattedTimestamps = formatTimestamps(findTimestamps(message));
        if (!formattedTimestamps.containsKey(ElasticsearchMessage.DEFAULT_TIMESTAMP_FIELD)) {
            formattedTimestamps.put(ElasticsearchMessage.DEFAULT_TIMESTAMP_FIELD, formatTimestamp(ZonedDateTime.now()));
        }

        sourceMap.putAll(formattedTimestamps);

        IndexRequest request = new IndexRequest(index.toString(), documentType).source(sourceMap);
        processor.add(request);
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(String.format("Added message to %s with data: %s", index, sourceMap));
        }
    }

    /**
     * Formats a {@link ZonedDateTime} to a string using the {@value #DATE_TIME_FORMATTER} {@link DateTimeFormatter}.
     *
     * @param timestamp the timestamp.
     * @return A String formatted via {@link #DATE_TIME_FORMATTER}.
     */
    static String formatTimestamp(ZonedDateTime timestamp) {
        return timestamp.format(DATE_TIME_FORMATTER);
    }

    private Map<String, ZonedDateTime> findTimestamps(ElasticsearchMessage message) {
        final TypeConstraint<ZonedDateTime> timestampConstraint = TypeConstraint.ofClass(ZonedDateTime.class);
        return message
                .toMap()
                .entrySet()
                .stream()
                .filter(entry -> timestampConstraint.canCast(entry.getValue()))
                .collect(
                        Collectors.<Map.Entry<String, Object>, String, ZonedDateTime> toMap(entry -> entry.getKey(),
                                entry -> timestampConstraint.cast(entry.getValue())));
    }

    /**
     * Formats the time-stamp fields in the given map by converting them to strings using the
     * {@link #formatTimestamp(ZonedDateTime)} method.
     *
     * @param timestamps A map of keys with associated {@link ZonedDateTime}s.
     * @return A map of keys and formatted time-stamps.
     */
    private Map<String, String> formatTimestamps(Map<String, ZonedDateTime> timestamps) {
        return timestamps.entrySet().stream().collect(Collectors.toMap(entry -> entry.getKey(), entry -> {
            return formatTimestamp(entry.getValue());
        }));
    }

    @Override
    public void close() throws IOException {
        processor.close();
    }

}

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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of a {@link BulkProcessor.Listener} that receives events when processing bulk data in the
 * {@link BulkConsumer}.
 *
 * @return A new listener using the slf4j logger
 */
public class BulkProcessorListener implements BulkProcessor.Listener {

    private static final String BULK_LOGGING_STRING = "%s bulk with %d actions. Id: %d Size: %d.";
    private static final Logger LOGGER = LoggerFactory.getLogger(BulkProcessorListener.class);

    /**
     * Counts messages sent to each indices, by grouping each request on its index and summing the number of
     * {@link IndexRequest}s for each index.
     * 
     * @param requests The list of requests to count.
     * @return A {@link Map} which describes the name of each index and the number of messages stored in that index.
     */
    private <T extends IndicesRequest> Map<String, Integer> countMessagesSentToIndices(List<T> requests) {
        return requests.stream().flatMap(indexRequest -> Stream.of(indexRequest.indices()))
                .collect(Collectors.groupingByConcurrent(Function.identity())).entrySet().stream()
                .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue().size()));
    }

    /**
     * Formats a logging string of the given verb (action), execution id and {@link BulkRequest}. If the debug level is
     * enabled in the {@link Logger} of this class, a richer string will be written where the message count for each
     * index is compiled.
     * 
     * @param verb The verb of the bulk, e. g. "Preparing".
     * @param executionId The sequential execution id of the {@link BulkRequest}.
     * @param request The actual {@link BulkRequest}.
     * @return A formatted {@link String}.
     */
    private String format(String verb, long executionId, BulkRequest request) {
        final String defaultLogString = String.format(BULK_LOGGING_STRING, verb, request.numberOfActions(),
                executionId, request.estimatedSizeInBytes());
        if (LOGGER.isDebugEnabled() || LOGGER.isTraceEnabled()) {
            List<? extends IndicesRequest> requests = request.subRequests();
            Map<String, Integer> messageCountPerIndex = countMessagesSentToIndices(requests);
            String groupedIndexRequests = messageCountPerIndex.entrySet().stream()
                    .map(e -> e.getKey() + " -> " + e.getValue()).collect(Collectors.joining("\n\t"));
            return defaultLogString + "\nIndices:\n\t" + groupedIndexRequests;
        } else {
            return defaultLogString;
        }
    }

    @Override
    public void beforeBulk(long executionId, BulkRequest request) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(format("Preparing", executionId, request));
        }
    }

    @Override
    public void afterBulk(long executionId, BulkRequest bulkRequest, BulkResponse response) {
        if (response.hasFailures()) {
            List<BulkItemResponse> list = Arrays.asList(response.getItems());

            LOGGER.error(String.format("Failures in bulk of size %d (expected size: %d)", list.size(),
                    bulkRequest.numberOfActions()));
            list.stream()
                    .filter(request -> request.isFailed())
                    .map(request -> String.format("Message with id %s to index %s failed with message: %s",
                            request.getId(), request.getIndex(), request.getFailureMessage())).forEach(LOGGER::error);
        } else {
            LOGGER.info(String.format("%s Time: %dms", format("Completed", executionId, bulkRequest),
                    response.getTookInMillis()));
        }

    }

    @Override
    public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
        LOGGER.error("Error when executing bulk: " + failure.getLocalizedMessage());
    }
}

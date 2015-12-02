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

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import cern.acet.tracing.Message;
import cern.acet.tracing.util.type.TypeConstraint;
import cern.acet.tracing.util.type.TypeStrategy;
import cern.acet.tracing.util.type.strategy.AcceptStrategy;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

/**
 * A {@link Message} that is specific to Elasticsearch by containing information about which index / indices it should
 * be stored in.
 *
 * @author jepeders
 */
public class ElasticsearchMessage extends Message<ElasticsearchMessage> {

    /**
     * The name of the timestamp key in {@link Message}s.
     */
    public static final String DEFAULT_TIMESTAMP_FIELD = "@timestamp";

    private final List<ElasticsearchIndex> indices;

    ElasticsearchMessage(Map<String, Object> newObjectMap, Map<String, TypeConstraint<?>> newTypeMap,
            TypeStrategy newStrategy, List<ElasticsearchIndex> indices) {
        super(newObjectMap, newTypeMap, newStrategy);
        this.indices = Collections.synchronizedList(new ArrayList<>(indices));
    }

    /**
     * Adds an {@link ElasticsearchIndex} to this message, adding the {@link Message} to the corresponding index in the
     * Elasticsearch cluster.
     *
     * @param index The {@link ElasticsearchIndex} signalling which Elasticsearch index to store this message in.
     * @return this message with the added index.
     */
    public ElasticsearchMessage addIndex(ElasticsearchIndex index) {
        indices.add(index);
        return this;
    }

    /**
     * Adds a number of {@link ElasticsearchIndex}es to this message, adding the {@link Message} to the corresponding
     * index in the Elasticsearch cluster.
     *
     * @param newIndices The {@link ElasticsearchIndex}es signalling which Elasticsearch indices to store this message
     *            in.
     * @return this message with the added indices.
     */
    public ElasticsearchMessage addIndices(List<ElasticsearchIndex> newIndices) {
        indices.addAll(newIndices);
        return this;
    }

    @Override
    public ElasticsearchMessage copy() {
        return new ElasticsearchMessage(new ConcurrentHashMap<>(toMap()), getTypeMap(), getTypeStrategy(), indices);
    }

    /**
     * Retrieves the target indices for the message, if any.
     *
     * @return An immutable list of strings. Can be empty.
     */
    public ImmutableList<ElasticsearchIndex> getIndices() {
        return ImmutableList.copyOf(indices);
    }

    @Override
    protected ElasticsearchMessage getThis() {
        return this;
    }

    /**
     * Gets the time-stamp of the {@link ElasticsearchTemplateMapping}.
     *
     * @return A {@link ZonedDateTime} if the timestamp has been set, otherwise {@link Optional#empty()}.
     */
    public Optional<ZonedDateTime> getTimestamp() {
        return getOptionalAs(DEFAULT_TIMESTAMP_FIELD, ZonedDateTime.class);
    }

    /**
     * Creates an empty {@link ElasticsearchMessage} where the values are bound to the given type-mapping.
     *
     * @param typeMap A key-value storage which defines which types should be stored in which keys.
     * @param typeStrategy A {@link TypeStrategy} which decides the behaviour when a type or key is not found.
     * @return An empty {@link ElasticsearchMessage}.
     */
    public static ElasticsearchMessage of(Map<String, TypeConstraint<?>> typeMap, TypeStrategy typeStrategy) {
        return new ElasticsearchMessage(new ConcurrentHashMap<>(), typeMap, typeStrategy, ImmutableList.of());
    }

    /**
     * Creates an empty {@link ElasticsearchMessage} without any type mapping. The given strategy decides what happens
     * when any field is inserted.
     *
     * @param strategy Defines the behaviour when fields are inserted in the message.
     * @return An {@link ElasticsearchMessage} without any type restrictions.
     */
    public static ElasticsearchMessage of(TypeStrategy strategy) {
        return of(ImmutableMap.of(), strategy);
    }

    /**
     * Creates an empty {@link ElasticsearchMessage} without any type mapping and where new fields are always accepted
     * as {@link Object} types ({@link AcceptStrategy});
     *
     * @return An {@link ElasticsearchMessage} without any type restrictions.
     */
    public static ElasticsearchMessage ofUntyped() {
        return of(ImmutableMap.of(), AcceptStrategy.INSTANCE);
    }

    /**
     * Sets the time-stamp and returns a new message, where any previous value are overwritten.
     *
     * @param timestamp A {@link ZonedDateTime} to set as the new time-stamp.
     * @return A new {@link Message} with an updated time-stamp.
     */
    @Override
    public ElasticsearchMessage putTimestamp(ZonedDateTime timestamp) {
        return put(DEFAULT_TIMESTAMP_FIELD, timestamp);
    }

    @Override
    public String toString() {
        return super.toString() + indices.toString();
    }

}
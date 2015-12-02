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

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import jdk.nashorn.internal.codegen.TypeMap;

import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.compress.CompressedXContent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cern.acet.tracing.Message;
import cern.acet.tracing.util.type.TypeConstraint;
import cern.acet.tracing.util.type.TypedMap;

import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 * This class can fetch and return a type mapping from a template in an Elasticsearch cluster. A type mapping is a map
 * that associated field-keys with value types, which can be used in {@link Message}s to restrict insertions of values
 * to the type (or a subtype) of the type specified in the type map.
 *
 * @author jepeders
 * @see TypedMap
 * @see Message
 */
public class ElasticsearchTemplateMapping implements ElasticsearchTypeMapping {

    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticsearchTemplateMapping.class);
    private static final String TYPE_FIELD = "type";
    private static final String PROPERTIES_FIELD = "properties";
    static final Duration UPDATE_INTERVAL = Duration.ofHours(1);

    /**
     * String to Class<?> conversions of Elasticsearch types.
     * 
     * @see https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-types.html
     */
    private static final ImmutableMap<String, Class<?>> TYPE_CONVERTER = ImmutableMap.<String, Class<?>>builder()
        //@formatter:off
            
        // Core datatypes
        .put("string", String.class)
        // Numeric
        .put("long", Long.class)
        .put("integer", Integer.class)
        .put("short", Short.class)
        .put("byte", Byte.class)
        .put("double", Double.class)
        .put("float", Float.class)
        // Boolean
        .put("boolean", Boolean.class)
        // Complex
        .put("object", Object.class)
        // Special
        .put("date", ZonedDateTime.class)
        
        .build();
        //@formatter:on

    private final String templateName;
    private final String mappingName;

    private final ReentrantReadWriteLock reentrantLock = new ReentrantReadWriteLock();
    private final Lock readLock = reentrantLock.readLock();
    private final Lock writeLock = reentrantLock.writeLock();

    private Instant lastCacheUpdate = Instant.EPOCH;
    private ImmutableMap<String, TypeConstraint<?>> typeMap = ImmutableMap.of();

    /**
     * Creates a {@link ElasticsearchTemplateMapping} that pairs keys with type information, extracted from the
     * Elasticsearch template and its mapping. If no template could be found, the mapping will be empty.
     *
     * @param templateName The name of the Elasticsearch template containing the type mapping.
     * @param mappingName The name of the type mapping within the template.
     */
    public ElasticsearchTemplateMapping(String templateName, String mappingName) {
        this.templateName = templateName;
        this.mappingName = mappingName;
    }

    /**
     * Retrieves the type mapping of this {@link ElasticsearchTemplateMapping}.
     *
     * @param client The Elasticsearch {@link Client} that is connected to a cluster.
     * @return An immutable {@link TypeMap}.
     */
    @Override
    public ImmutableMap<String, TypeConstraint<?>> getTypeMap(Client client) {
        return getTypeMap(client.admin().indices(), Clock.systemDefaultZone());
    }

    /**
     * Retrieves the type mapping of this {@link ElasticsearchTemplateMapping}.
     *
     * @param client The Elasticsearch {@link IndicesAdminClient} that is connected to a cluster.
     * @param clock A clock that determines the time of the cache.
     * @return An immutable {@link TypeMap}.
     */
    ImmutableMap<String, TypeConstraint<?>> getTypeMap(IndicesAdminClient client, Clock clock) {
        if (lastCacheUpdate.isBefore(Instant.now(clock).minus(UPDATE_INTERVAL))) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Cache expired, fetching new mapping");
            }
            if (writeLock.tryLock()) {
                try {
                    this.typeMap = fetchNewTypeMap(client);
                    lastCacheUpdate = Instant.now(clock);
                } finally {
                    writeLock.unlock();
                }
            }
        } else if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("Cached mapping returned: {}", typeMap);
        }

        readLock.lock();
        try {
            return this.typeMap;
        } finally {
            readLock.unlock();
        }
    }

    private ImmutableMap<String, TypeConstraint<?>> fetchNewTypeMap(IndicesAdminClient client) {
        try {
            GetIndexTemplatesResponse response = client.prepareGetTemplates(templateName).get();
            Map<String, String> typeMapping = extractMappings(response);
            ImmutableMap.Builder<String, TypeConstraint<?>> builder = ImmutableMap.builder();
            typeMapping.forEach((key, value) -> {
                builder.put(key, TypeConstraint.ofClass(TYPE_CONVERTER.get(value)));
            });
            return builder.build();
        } catch (Exception e) {
            LOGGER.warn("Failed to fetch Elasticsearch mapping. Reverting to previous mapping {}.", typeMap, e);
            return typeMap;
        }
    }

    /**
     * Extract a type-mapping as a {@link Map} from the given responce.
     * 
     * @param response A {@link GetIndexTemplatesResponse}.
     * @return A map of {@link String} to {@link String}, containing
     */
    private Map<String, String> extractMappings(GetIndexTemplatesResponse response) {
        List<IndexTemplateMetaData> list = response.getIndexTemplates();
        ImmutableOpenMap<String, CompressedXContent> mappings = list.get(0).getMappings();
        CompressedXContent templateMappings = mappings.get(mappingName);
        JsonElement parser = new JsonParser().parse(templateMappings.toString());
        JsonObject object = parser.getAsJsonObject();
        JsonObject types = object.getAsJsonObject(mappingName).getAsJsonObject(PROPERTIES_FIELD);

        Stream<Entry<String, JsonElement>> stream = types.entrySet().stream();
        return stream.collect(Collectors.toMap(entry -> entry.getKey(), entry -> {
            return entry.getValue().getAsJsonObject().get(TYPE_FIELD).getAsString();
        }));
    }

}

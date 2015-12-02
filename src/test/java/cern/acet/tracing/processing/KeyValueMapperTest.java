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

package cern.acet.tracing.processing;

import static org.junit.Assert.assertEquals;

import java.time.ZonedDateTime;

import org.junit.Before;
import org.junit.Test;

import cern.acet.tracing.output.elasticsearch.ElasticsearchMessage;
import cern.acet.tracing.util.type.TypeConstraint;
import cern.acet.tracing.util.type.strategy.AcceptStrategy;

import com.google.common.collect.ImmutableMap;

public class KeyValueMapperTest {

    private static final String BODY_FIELD = "body";

    private static final ImmutableMap<String, TypeConstraint<?>> TYPE_MAP = ImmutableMap.of(
            ElasticsearchMessage.DEFAULT_TIMESTAMP_FIELD, TypeConstraint.ofClass(ZonedDateTime.class), BODY_FIELD,
            TypeConstraint.ofClass(String.class));

    private KeyValueMapper<ElasticsearchMessage> keyValueMapper;
    private ElasticsearchMessage elasticsearchMessage;

    @Before
    public void setup() {
        elasticsearchMessage = ElasticsearchMessage.of(TYPE_MAP, AcceptStrategy.INSTANCE);
        keyValueMapper = KeyValueMapper.<ElasticsearchMessage> builder().setFieldToParse(BODY_FIELD)
                .setKeyValueParser((message, tuple) -> message.put(tuple.getFirst(), tuple.getSecond())).build();
    }

    @Test
    public void canParseMessageWithoutBody() {
        assertEquals(elasticsearchMessage, parse(elasticsearchMessage));
    }

    @Test
    public void canParseMessageWithEmptyBody() {
        ElasticsearchMessage message = elasticsearchMessage.put(BODY_FIELD, "");
        assertEquals(message, parse(message));
    }

    @Test
    public void canParseKeyValuePair() {
        ElasticsearchMessage message = elasticsearchMessage.put(BODY_FIELD, "key=value");
        assertEquals("value", parse(message).get("key"));
    }

    @Test
    public void canLeaveBodyFieldAfterParsing() {
        String body = "key=value";
        ElasticsearchMessage message = elasticsearchMessage.put(BODY_FIELD, body);
        assertEquals(body, parse(message).get(BODY_FIELD));
    }

    private ElasticsearchMessage parse(ElasticsearchMessage message) {
        return keyValueMapper.apply(message);
    }

}

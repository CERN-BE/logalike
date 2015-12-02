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

import static org.junit.Assert.assertEquals;

import java.time.ZonedDateTime;

import org.junit.Before;
import org.junit.Test;

import cern.acet.tracing.util.type.TypeConstraint;
import cern.acet.tracing.util.type.strategy.AcceptStrategy;
import cern.acet.tracing.util.type.strategy.ThrowingStrategy;

import com.google.common.collect.ImmutableMap;

public class ElasticsearchMessageTest {

    private static final ImmutableMap<String, TypeConstraint<?>> DEFAULT_TYPE = ImmutableMap.of(
            ElasticsearchMessage.DEFAULT_TIMESTAMP_FIELD, TypeConstraint.ofClass(ZonedDateTime.class));
    private ElasticsearchMessage message;

    @Before
    public void setup() {
        message = ElasticsearchMessage.of(DEFAULT_TYPE, ThrowingStrategy.INSTANCE);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void canCreateTypedMessage() {
        message.put("test", 1);
    }

    @Test
    public void canCreateUntypedMessage() {
        String key = "test";
        Integer value = 2;
        assertEquals(value, ElasticsearchMessage.of(AcceptStrategy.INSTANCE).put(key, value).getAs(key, Object.class));
    }

    @Test
    public void canInsertTimestamp() {
        ZonedDateTime value = ZonedDateTime.now();
        assertEquals(value, message.putTimestamp(value).toMap().get(ElasticsearchMessage.DEFAULT_TIMESTAMP_FIELD));
    }

}

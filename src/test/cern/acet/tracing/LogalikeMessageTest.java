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

package cern.acet.tracing;

import static org.junit.Assert.assertEquals;

import java.util.concurrent.ConcurrentHashMap;

import org.junit.Before;
import org.junit.Test;

import cern.acet.tracing.util.type.TypeConstraint;
import cern.acet.tracing.util.type.strategy.DropStrategy;

import com.google.common.collect.ImmutableMap;

public class LogalikeMessageTest {

    private static final String HOST_KEY = "host";
    private static final ImmutableMap<String, TypeConstraint<?>> HOST_TYPE_MAP = ImmutableMap.of(HOST_KEY,
            TypeConstraint.ofClass(String.class));

    Message<?> message;

    final static String TEST_HOST = "testHost";

    @Before
    public void setup() {
        message = new MessageImpl(new ConcurrentHashMap<>(), HOST_TYPE_MAP, DropStrategy.INSTANCE);
    }

    @Test
    public void canCreateEmptyMessage() {
        assertEquals(0, message.toMap().size());
    }

    @Test
    public void canInsertHost() {
        final String value = "testHost";
        assertEquals(value, message.put(HOST_KEY, value).toMap().get(HOST_KEY));
    }

}

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

import java.time.ZonedDateTime;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import cern.acet.tracing.util.type.TypeConstraint;
import cern.acet.tracing.util.type.TypeStrategy;
import cern.acet.tracing.util.type.strategy.AcceptStrategy;

import com.google.common.collect.ImmutableMap;

public class MessageImpl extends Message<MessageImpl> {

    public static final String TIMESTAMP_FIELD = "@timestamp";

    public MessageImpl() {
        this(AcceptStrategy.INSTANCE);
    }

    public MessageImpl(TypeStrategy strategy) {
        this(new ConcurrentHashMap<>(), ImmutableMap.of(), strategy);
    }

    public MessageImpl(ConcurrentHashMap<String, Object> newObjectMap, Map<String, TypeConstraint<?>> newTypeMap,
            TypeStrategy newStrategy) {
        super(newObjectMap, newTypeMap, newStrategy);
    }

    public ZonedDateTime getTimestamp() {
        return getAs(TIMESTAMP_FIELD, ZonedDateTime.class);
    }

    @Override
    public MessageImpl putTimestamp(ZonedDateTime timestamp) {
        return put(TIMESTAMP_FIELD, timestamp);
    }

    @Override
    protected MessageImpl getThis() {
        return this;
    }

    @Override
    public MessageImpl copy() {
        return new MessageImpl(new ConcurrentHashMap<>(toMap()), getTypeMap(), getTypeStrategy());
    }
}
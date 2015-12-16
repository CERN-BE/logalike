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

import cern.acet.tracing.util.type.TypeConstraint;
import cern.acet.tracing.util.type.TypeStrategy;
import cern.acet.tracing.util.type.strategy.AcceptStrategy;
import com.google.common.collect.ImmutableMap;

import java.time.ZonedDateTime;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A simple implementation of a {@link Message}.
 *
 * @author ghoranyi, jepeders
 */
public class MessageImpl extends Message<MessageImpl> {

    public static final String TIMESTAMP_FIELD = "timestamp";

    protected MessageImpl(Map<String, Object> objectMap, Map<String, TypeConstraint<?>> typeMap,
                          TypeStrategy strategy) {
        super(objectMap, typeMap, strategy);
    }

    @Override
    protected MessageImpl getThis() {
        return this;
    }

    /**
     * Copies this message into a new instance. The underlying data is copied so the new message will be completely
     * separated from the old.
     * 
     * @return A copy of this {@link MessageImpl}.
     */
    public MessageImpl copy() {
        return new MessageImpl(new ConcurrentHashMap<>(toMap()), getTypeMap(), getTypeStrategy());
    }

    /**
     * Creates an empty {@link MessageImpl} without any type mapping. The given strategy decides what happens
     * when any field is inserted.
     *
     * @param strategy Defines the behaviour when fields are inserted in the message.
     * @return An {@link MessageImpl} without any type restrictions.
     */
    public static MessageImpl of(TypeStrategy strategy) {
        return new MessageImpl(new ConcurrentHashMap<>(), ImmutableMap.of(), strategy);
    }

    /**
     * Creates an empty {@link MessageImpl} without any type mapping and where new fields are always accepted
     * as {@link Object} types ({@link AcceptStrategy});
     *
     * @return An {@link MessageImpl} without any type restrictions.
     */
    public static MessageImpl ofUntyped() {
        return of(AcceptStrategy.INSTANCE);
    }

    /**
     * Sets the time-stamp for this message.
     * 
     * @param timestamp The new time-stamp.
     * @return A message with the given time-stamp.
     */
    public MessageImpl putTimestamp(ZonedDateTime timestamp) {
        return put(TIMESTAMP_FIELD, timestamp);
    }

}

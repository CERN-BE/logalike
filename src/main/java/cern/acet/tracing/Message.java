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

import javax.xml.soap.MessageFactory;

import cern.acet.tracing.util.type.TypeConstraint;
import cern.acet.tracing.util.type.TypeStrategy;
import cern.acet.tracing.util.type.TypedMap;

import com.google.common.collect.ImmutableMap;

/**
 * <p>
 * An abstract and immutable message which stores key-value data in an {@link ImmutableMap}. The {@link Message} uses a
 * type-map that determines the type for each field at runtime. Because of the {@link TypedMap}, the message instance
 * cannot be instantiated directly, but should be created via the {@link MessageFactory} instead.
 * </p>
 * <h3>Type mapping</h3>
 * <p>
 * Type mapping exists to ensure that the data has the same type while being processed in Logsmart, as they will have
 * in the actual storage. For some systems (such as Elasticsearch) this is vital to the search capabilities. However, it
 * is possible to completely skip this by using the {@link Message} constructor with no arguments.
 * </p>
 * <h3>Time-stamps</h3>
 * <p>
 * Because messages are often associated with time, this abstract class also provides a way to set the time-stamp of the
 * message via the {@link #putTimestamp(ZonedDateTime)} method.
 * </p>
 * 
 * @param <MessageType> The type that is inherited from a {@link Message}. The type is parameterised, so methods such as
 *            {@link #put(Map)} will return the implementation instead of a {@link Message}.
 * @author ghoranyi, jepeders
 */
public abstract class Message<MessageType extends Message<MessageType>> extends TypedMap<MessageType> {

    protected Message(Map<String, Object> objectMap, Map<String, TypeConstraint<?>> typeMap,
            TypeStrategy strategy) {
        super(objectMap, typeMap, strategy);
    }

    /**
     * Copies this message into a new instance. The underlying data is copied so the new message will be completely
     * separated from the old.
     * 
     * @return A copy of this {@link Message}.
     */
    public abstract MessageType copy();

    /**
     * Sets the time-stamp for this message.
     * 
     * @param timestamp The new time-stamp.
     * @return A message with the given time-stamp.
     */
    public abstract MessageType putTimestamp(ZonedDateTime timestamp);

}

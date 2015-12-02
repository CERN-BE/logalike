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

import java.util.function.UnaryOperator;

import cern.acet.tracing.Message;

/**
 * A mapper that performs an operation on every incoming message that returns a new instance of the incoming message.
 *
 * @param <MessageType> The type of {@link Message} which can be subtyped for additional functionality.
 * @author jepeders
 */
public interface MessageMapper<MessageType extends Message<MessageType>> extends UnaryOperator<MessageType> {

    /**
     * Applies the operation of this mapper to the incoming {@link Message}.
     *
     * @param message The message to alter.
     * @return An instance of a {@link Message}, transformed with the mapper function. Can be the same {@link Message}
     *         instance if the mapper does not alter the message content.
     */
    @Override
    MessageType apply(MessageType message);

}

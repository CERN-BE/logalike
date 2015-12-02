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

import java.io.Closeable;
import java.util.function.Consumer;

/**
 * An output for Logstash capable of consuming messages into a sink, specified in the actual implementation.
 * 
 * @param <MessageType> The type of {@link Message} which can be subtyped to extend the functionality.
 * @author ghoranyi, jepeders
 */
public interface Output<MessageType extends Message<MessageType>> extends Consumer<MessageType>, Closeable {

    /**
     * Accepts a message and sends it to the relevant output sink.
     * 
     * @param message The message to consume.
     * @param metrics The metrics to register statistics about the message handling
     */
    @Override
    void accept(MessageType message);

}

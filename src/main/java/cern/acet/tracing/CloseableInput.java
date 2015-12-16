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
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * A Logstash input capable of serving {@link Message}s from a stream. Depending on the implementation this stream can
 * be parallel or sequential. Unlike the regular {@link Input}, inputs built
 * with this interface will be closed when Logalike closes.
 * 
 * @param <MessageType> The type of {@link Message} which can be subtyped to extend the functionality.
 * @author ghoranyi, jepeders
 */
public interface CloseableInput<MessageType extends Message<MessageType>> extends Supplier<Stream<MessageType>>,
        Input<MessageType>, Closeable {

    /**
     * Returns a stream of {@link Message}s generated from this input.
     * 
     * @return a {@link Stream} of {@link Message}s.
     */
    @Override
    Stream<MessageType> get();

}

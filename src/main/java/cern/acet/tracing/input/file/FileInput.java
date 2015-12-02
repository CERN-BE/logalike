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

package cern.acet.tracing.input.file;

import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cern.acet.tracing.Input;
import cern.acet.tracing.Message;

/**
 * An {@link Input} that can read and parse lines from one or more {@link File}s.
 *
 * @author jepeders
 */
public class FileInput<MessageType extends Message<MessageType>> implements Input<MessageType> {

    private static final Logger LOGGER = LoggerFactory.getLogger(FileInput.class);
    private final Stream<MessageType> messageStream;
    private final AutoCloseable closeable;
    private final ExecutorService executor;

    FileInput(FileInputBuilder<MessageType, ?> builder, ExecutorService executor) {
        this.executor = executor;
        this.messageStream = builder.getStream();
        this.closeable = builder.getCloseableHook();
    }

    /**
     * Creates a {@link TailingBuilder} that can construct a {@link FileInput} that tails one or more files.
     *
     * @param converter A function to convert the lines in a file into {@link Message}s.
     * @return A {@link TailingBuilder} that can construct a {@link FileInput}.
     */
    public static <MessageType extends Message<MessageType>> TailingBuilder<MessageType> buildTailing(
            Function<String, MessageType> converter) {
        return new TailingBuilder<>(converter);
    }

    @Override
    public void close() {
        try {
            closeable.close();
            executor.shutdown();
        } catch (Exception e) {
            LOGGER.warn("Error when closing file input", e);
        }
    }

    @Override
    public Stream<MessageType> get() {
        return messageStream;
    }

}
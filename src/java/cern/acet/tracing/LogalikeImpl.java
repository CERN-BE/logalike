/**
 * Logalike - A stream based message processor Copyright (c) 2015 European Organisation for Nuclear Research (CERN), All
 * Rights Reserved. This software is distributed under the terms of the GNU General Public Licence version 3 (GPL
 * Version 3), copied verbatim in the file “COPYLEFT”. In applying this licence, CERN does not waive the privileges and
 * immunities granted to it by virtue of its status as an Intergovernmental Organization or submit itself to any
 * jurisdiction. Authors: Gergő Horányi <ghoranyi> and Jens Egholm Pedersen <jegp>
 */

package cern.acet.tracing;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cern.acet.tracing.processing.Processor;

/**
 * An implementation of {@link Logalike} that reads messages from the {@link Input}, processes them with the given
 * {@link Processor}s and sends them to the {@link Output}. This implementation assumes that the {@link Stream} from the
 * input source is endless. The {@link LogalikeImpl} is closed by short-circuiting the stream and calling its
 * closing-handlers. {@link Input}s and {@link Output}s should therefore use the stream's
 * {@link Stream#onClose(Runnable)} method to finalise any dangling resources.
 *
 * @param <MessageType> The type of {@link Message} to process.
 * @author ghoranyi, jepeders
 */
public class LogalikeImpl<MessageType extends Message<MessageType>> implements Logalike<MessageType> {

    private static final Logger LOGGER = LoggerFactory.getLogger(LogalikeImpl.class);

    private final Input<MessageType> input;
    private final Output<MessageType> output;
    private final Function<Stream<MessageType>, Stream<MessageType>> processorChain;

    private final AtomicBoolean isClosed = new AtomicBoolean(false);

    private Stream<MessageType> inputStream = null;
    private Stream<MessageType> outputStream = null;

    /**
     * Creates a new Logalike instance.
     *
     * @param input The {@link Input} that generates {@link Message}s.
     * @param output The {@link Output} that can consume {@link Message}s.
     * @param processor The {@link Processor} that can process {@link Message}s.
     * @throws NullPointerException if any of the parameters are null
     */
    private LogalikeImpl(Builder<MessageType> builder) throws NullPointerException {
        this.input = builder.input;
        this.output = builder.output;
        this.processorChain = builder.processorChain;
    }

    /**
     * Creates a new {@link cern.acet.tracing.Logalike.Builder} that can be used to build an instance of
     * {@link LogalikeImpl}.
     *
     * @return An implementation of a {@link cern.acet.tracing.Logalike.Builder}.
     */
    public static <MessageType extends Message<MessageType>> Builder<MessageType> builder() {
        return new Builder<MessageType>();
    }

    /**
     * Retrieves the {@link Processor} (reduced to one single {@link UnaryOperator}, which in turn is a subclass of
     * {@link Function} with the same in- and output parameters) used by this {@link Logalike} instance.
     *
     * @return A {@link Function} that processes all the incoming {@link Message}s.
     */
    public Processor<MessageType> getProcessorChain() {
        return processorChain::apply;
    }

    @Override
    public void run() {
        inputStream = input.get().parallel().peek(message -> {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("Received message: " + message);
            }
        });

        outputStream = processorChain.apply(inputStream);

        outputStream.peek(message -> {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("Sending message to output: " + message);
            }

            if (isClosed.get()) {
                throw new RuntimeException("Stream closed");
            }
        }).peek(output::accept)
        /* Short-circuit this stream if Logalike is closing */
        .anyMatch(m -> isClosed.get());
    }

    @Override
    public void close() throws IOException {
        isClosed.set(true);
        if (inputStream != null) {
            inputStream.close();
        }
        if (outputStream != null) {
            outputStream.close();
        }
        input.close();
        output.close();
    }

    /**
     * A {@link cern.acet.tracing.Logalike.Builder} for the {@link LogalikeImpl} class.
     *
     * @param <MessageType> The type of {@link Message} to build a {@link Logalike} implemetation over..
     * @author jepeders
     */
    public static class Builder<MessageType extends Message<MessageType>> implements Logalike.Builder<MessageType> {

        private Input<MessageType> input;
        private Output<MessageType> output;
        private Function<Stream<MessageType>, Stream<MessageType>> processorChain = stream -> stream;

        @Override
        public LogalikeImpl<MessageType> build() {
            if (input == null) {
                throw new IllegalArgumentException("Input must be defined");
            }
            if (output == null) {
                throw new IllegalArgumentException("Output must be defined");
            }
            return new LogalikeImpl<MessageType>(this);
        }

        @Override
        public Builder<MessageType> addFilter(Predicate<MessageType> filter) {
            processorChain = processorChain.andThen(Processor.ofPredicate(filter));
            return this;
        }

        @Override
        public Builder<MessageType> addMapper(UnaryOperator<MessageType> mapper) {
            processorChain = processorChain.andThen(Processor.ofMapper(mapper));
            return this;
        }

        @Override
        public Builder<MessageType> addProcessor(Processor<MessageType> processorToAdd) {
            processorChain = processorChain.andThen(processorToAdd);
            return this;
        }

        @Override
        public Builder<MessageType> setInput(Input<MessageType> input) {
            this.input = input;
            return this;
        }

        @Override
        public Builder<MessageType> setOutput(Output<MessageType> output) {
            this.output = output;
            return this;
        }

    }

}

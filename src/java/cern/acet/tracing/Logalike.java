/**
 * Logalike - A stream based message processor Copyright (c) 2015 European Organisation for Nuclear Research (CERN), All
 * Rights Reserved. This software is distributed under the terms of the GNU General Public Licence version 3 (GPL
 * Version 3), copied verbatim in the file “COPYLEFT”. In applying this licence, CERN does not waive the privileges and
 * immunities granted to it by virtue of its status as an Intergovernmental Organization or submit itself to any
 * jurisdiction. Authors: Gergő Horányi <ghoranyi> and Jens Egholm Pedersen <jegp>
 */

package cern.acet.tracing;

import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

import cern.acet.tracing.output.elasticsearch.ElasticsearchMessage;
import cern.acet.tracing.processing.Filter;
import cern.acet.tracing.processing.MessageMapper;
import cern.acet.tracing.processing.Processor;

/**
 * <p>
 * An interface that runs Logalike. It is built using a builder pattern (via {@link Builder}) and uses Java 8
 * {@link Stream}s to process messages in three stages.
 * </p>
 * <ol>
 * <li>{@link Message}s are fetched as a {@link Stream} from the given {@link Input}.</li>
 * <li>The stream is processed using one or more {@link Processor}s .</li>
 * <li>Messages are sent to the {@link Output} one by one.</li>
 * </ol>
 *
 * @param <T> The type of {@link Message} to process in this Logalike instance. The {@link Message} type is allowed
 *            subtyped to include additional functionality for different {@link Message}-types such as the
 *            {@link ElasticsearchMessage}.
 * @author ghoranyi, jepeders
 */
public interface Logalike<MessageType extends Message<MessageType>> extends AutoCloseable, Runnable {

    /**
     * Create a {@link Builder} that can build a {@link Logalike} instance.
     *
     * @return An instance of the default {@link Builder}, used to construct a {@link Logalike} implementation.
     */
    static <MessageType extends Message<MessageType>> LogalikeImpl.Builder<MessageType> builder() {
        return LogalikeImpl.<MessageType> builder();
    }

    /**
     * Starts the {@link Logalike} instance by taking messages from the input, processing them and sending them to the
     * output. This method will run until the {@link Logalike} instance is {@link #close()}d.
     */
    @Override
    void run();

    /**
     * An interface for builders that can build {@link Logalike} instances.
     *
     * @param <MessageType> The type of {@link Message} to process in this Logalike instance. The {@link Message} type
     *            is allowed subtyped to include additional functionality for different {@link Message}-types such as
     *            the {@link ElasticsearchMessage}.
     * @author jepeders
     */
    interface Builder<MessageType extends Message<MessageType>> {

        /**
         * Adds a single {@link Filter} (subclass of {@link Predicate}) in the builder. Filters are applied sequentially
         * in the order they were added, but should be applied as soon as possible, to filter out {@link Message}s
         * before they are processed, thereby doing unnecessary calculations.
         *
         * @param filter The filter to add.
         * @return The same builder instance for use in chaining calls.
         */
        Builder<MessageType> addFilter(Predicate<MessageType> filter);

        /**
         * Add a {@link MessageMapper} (subclass to {@link UnaryOperator}) to the builder. Mappers are applied
         * sequentially in the order they arrive.
         *
         * @param mappers A {@link MessageMapper}.
         * @return The same builder instance for use in chaining calls.
         */
        Builder<MessageType> addMapper(UnaryOperator<MessageType> mapper);

        /**
         * Add a {@link Processor} (subclass to {@link UnaryOperator}) to the builder. Processors are applied
         * sequentially in the order they arrive.
         *
         * @param processor A {@link Processor}.
         * @return The same builder instance for use in chaining calls.
         */
        Builder<MessageType> addProcessor(Processor<MessageType> processor);

        /**
         * Constructs an instance of Logalike from the fields defined in this builder so far.
         *
         * @return An runnable implementation of Logalike.
         * @throws IllegalArgumentException if the input or output was not yet defined in this builder (they cannot be
         *             null).
         */
        Logalike<MessageType> build() throws IllegalArgumentException;

        /**
         * Sets the {@link Input} of the builder.
         *
         * @param input An instance of a {@link Input}.
         * @return The same builder for use in chaining calls.
         */
        Builder<MessageType> setInput(Input<MessageType> input);

        /**
         * Sets the {@link Output} of the builder.
         *
         * @param output An instance of a {@link Output}.
         * @return The same builder for use in chaining calls.
         */
        Builder<MessageType> setOutput(Output<MessageType> output);

    }

}

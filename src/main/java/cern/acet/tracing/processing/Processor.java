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

import java.util.List;
import java.util.Objects;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

import cern.acet.tracing.Message;

/**
 * An interface for processing {@link Stream}s of {@link Message}s. A {@link Processor} can manipulate a stream in any
 * way it wants, but care should be taken when doing terminal operations; the incoming stream is most likely infinite.
 * Note that removing messages should be done via {@link Filter} operations wherever possible.
 *
 * @param <MessageType> The type of {@link Message} which can be subtyped to extend the functionality.
 * @author jepeders
 */
@FunctionalInterface
public interface Processor<MessageType extends Message<MessageType>> extends UnaryOperator<Stream<MessageType>> {

    /**
     * Returns a composed processor that first applies this processor to its input, and then applies the {@code after}
     * processor to the result. If evaluation of either function throws an exception, it is relayed to the caller of the
     * composed function.
     *
     * @param after the processor to apply after this processor is applied
     * @return a composed processor that first applies this processor and then applies the {@code after} processor
     * @throws NullPointerException if after is null
     */
    default Processor<MessageType> andThen(Processor<MessageType> after) {
        Objects.requireNonNull(after);
        return stream -> after.apply(apply(stream));
    }

    /**
     * Creates a reducer that can reduce {@link Processor}s, that processes the same type of {@link Message}s as this
     * {@link Processor}.
     *
     * @param <MessageType> The type of {@link Message} to create a reducer over.
     * @return A {@link BinaryOperator} that can reduce two {@link Processor}s into one.
     */
    static <MessageType extends Message<MessageType>> BinaryOperator<Processor<MessageType>> getProcessorReducer() {
        return (processor1, processor2) -> processor1.andThen(processor2);
    }

    /**
     * Creates an identity {@link Processor} that simply returns the input stream.
     *
     * @return An identity {@link Processor}.
     * @see Function#identity()
     */
    static <MessageType extends Message<MessageType>> Processor<MessageType> identity() {
        return stream -> stream;
    }

    /**
     * Creates a {@link Processor} from the given {@link Function} ({@link MessageMapper} and {@link ConditionalMapper}
     * are both subtypes of the {@link Function}-type {@link UnaryOperator}) by mapping the messages in the processor
     * stream with he operator.
     *
     * @param mapper The mapper to apply to all messages.
     * @param <MessageType> The type of {@link Message} to create a {@link Processor} over.
     * @return An instance of a {@link Processor} where all messages are mapped with the mapper.
     */
    static <MessageType extends Message<MessageType>> Processor<MessageType> ofMapper(
            Function<MessageType, MessageType> mapper) {
        return stream -> stream.map(mapper);
    }

    /**
     * Creates a {@link Processor} from the given {@link Predicate}, so that all messages in the stream given to the
     * processor will be filtered by the predicate.
     *
     * @param predicate The predicate used to filter {@link Message}s.
     * @param <MessageType> The type of {@link Message} to create a {@link Processor} over.
     * @return An instance of a {@link Processor}.
     */
    static <MessageType extends Message<MessageType>> Processor<MessageType> ofPredicate(
            Predicate<MessageType> predicate) {
        return stream -> stream.filter(predicate);
    }

    /**
     * Reduces a number of {@link Processor}s into a single Processor that takes an input {@link Stream} and runs it
     * sequentially through the given processors in the order they appear in the list.
     *
     * @param processors The {@link Processor}s to reduce.
     * @return A {@link Processor} which uses {@link #andThen(Function)} to execute all the processors sequentially.
     */
    static <MessageType extends Message<MessageType>> Processor<MessageType> ofProcessors(
            List<Processor<MessageType>> processors) {
        return processors.stream().reduce(getProcessorReducer()).orElse(Processor.identity());
    }

}

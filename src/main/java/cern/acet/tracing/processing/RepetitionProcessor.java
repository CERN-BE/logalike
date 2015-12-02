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

import java.time.Duration;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cern.acet.tracing.Message;
import cern.acet.tracing.processing.window.FingerprintStrategy;
import cern.acet.tracing.processing.window.MessageWindow;
import cern.acet.tracing.processing.window.WindowManager;

/**
 * <p>
 * A {@link Processor} that searches for repeating {@link Message}s in a specified time window. Two {@link Message}s are
 * considered to be repetitions of each other, if they have the same 'fingerprint' (using the functional interface
 * {@link FingerprintStrategy}). Repeating messages are compressed into a single message for each time-window. In that
 * message is a field ({@value #REPEATED_FIELD}) with a boolean value of <code>true</code> which indicates that the
 * message has been seen more than once in the time-window, and <code>false</code> if not. Another field (
 * {@value #REPEAT_COUNT_FIELD}) counts how many repetitions occurred in the time-window.
 * </p>
 * <p>
 * Both repeating messages and non-repeating messages are operated upon by a function or mapper ({@link UnaryOperator})
 * which can alter the message in any way necessary. By default the behaviour is to return the same message, but that
 * can be changed via {@link Builder#setRepeatingMapper(UnaryOperator)} and
 * {@link Builder#setNonRepeatingMapper(UnaryOperator)}.
 * </p>
 *
 * @param <T> The type of {@link Message} to process.
 * @author ghoranyi, jepeders
 */
public class RepetitionProcessor<T extends Message<T>> implements Processor<T> {

    private static final String NON_REPEATING_ACTION = "Aggregating";
    private static final String REPEATING_ACTION = "Repeating";
    private static final String REPEAT_COUNT_FIELD = "repeatCount";
    private static final String REPEATED_FIELD = "isRepeated";
    private static final Logger LOGGER = LoggerFactory.getLogger(RepetitionProcessor.class);

    private final UnaryOperator<T> nonRepetitionMapper;
    private final UnaryOperator<T> repetitionMapper;
    private final WindowManager<T> windowManager;

    /**
     * Creates a {@link Processor} that can detects and groups duplicates within a time-window.
     *
     * @param builder A {@link Builder} with all the necessary ingredients to create a {@link RepetitionProcessor}.
     */
    private RepetitionProcessor(Builder<T> builder) {
        this.windowManager = new WindowManager<T>(builder.windowDuration, builder.strategy);
        this.nonRepetitionMapper = builder.nonRepetitionMapper;
        this.repetitionMapper = builder.repetitionMapper;

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Repetition processor initialised with a window duration of {}", builder.windowDuration);
        }
    }

    @Override
    public Stream<T> apply(Stream<T> stream) {
        //@formatter:off
        Stream<T> nonRepeatedStream = stream
                .peek(windowManager::increment)
                .map(nonRepetitionMapper)
                .peek(traceMessage(REPEATING_ACTION));
        Stream<T> repeatedStream = windowManager.getClosedWindowsStream()
                .map(this::repetitionMapper)
                .map(repetitionMapper)
                .peek(traceMessage(NON_REPEATING_ACTION));
        //@formatter:on
        return Stream.concat(nonRepeatedStream, repeatedStream);
    }

    /**
     * @return A builder that can help build a {@link RepetitionProcessor}.
     * @param <T> The type of {@link Message} to process in the {@link RepetitionProcessor}.
     */
    public static <T extends Message<T>> Builder<T> builder() {
        return new Builder<T>();
    }

    /**
     * Sets the repeated field, repeat count and aggregated index to the given message.
     *
     * @param window The window to register in
     * @return The same {@link Message} that opened the window with the fields and index added.
     */
    private T repetitionMapper(MessageWindow<T> window) {
        long counter = window.getCount();
        T message = window.getMessage();
        if (counter > 1) {
            message = message.put(REPEATED_FIELD, true);
        } else {
            message = message.put(REPEATED_FIELD, false);
        }
        return message.put(REPEAT_COUNT_FIELD, counter);
    }

    private Consumer<T> traceMessage(String action) {
        return message -> {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace(action + " message: " + message);
            }
        };
    }

    /**
     * A builder that can help build a {@link RepetitionProcessor}.
     * 
     * @author jepeders
     * @param <T> The type of {@link Message}s to process in the built {@link RepetitionProcessor}.
     */
    public static class Builder<T extends Message<T>> {

        private static final Duration DEFAULT_WINDOW_DURATION = Duration.ofMinutes(2);
        private FingerprintStrategy<T> strategy;
        private Duration windowDuration = DEFAULT_WINDOW_DURATION;
        private UnaryOperator<T> nonRepetitionMapper = UnaryOperator.identity();
        private UnaryOperator<T> repetitionMapper = UnaryOperator.identity();

        /**
         * Builds an instance of a {@link RepetitionProcessor} using the current values of the builder.
         * 
         * @return A {@link RepetitionProcessor}
         * @throws IllegalArgumentException If the {@link FingerprintStrategy} is not set via
         *             {@link #setFingerprintStrategy(FingerprintStrategy)}.
         */
        public RepetitionProcessor<T> build() {
            if (strategy == null) {
                throw new IllegalArgumentException("FingerprintStrategy must be set");
            }
            return new RepetitionProcessor<T>(this);
        }

        /**
         * @param strategy A functional interface, that can define a 'fingerprint' for incoming {@link Message}s. If two
         *            messages has the same fingerprint they're considered equal.
         * @return The same builder with its {@link FingerprintStrategy} set.
         */
        public Builder<T> setFingerprintStrategy(FingerprintStrategy<T> strategy) {
            this.strategy = strategy;
            return this;
        }

        /**
         * Sets the {@link FingerprintStrategy} to use the value of fields with the given name. If no value is found, we
         * default to an empty string ("").
         * 
         * @param fieldName The name of the field, whose value to use for the {@link FingerprintStrategy}.
         * @return The same builder with its {@link FingerprintStrategy} set.
         */
        public Builder<T> setFingerprintStrategyByField(String fieldName) {
            this.strategy = message -> message.getOptionalAs(fieldName, String.class).orElse("");
            return this;
        }

        /**
         * @param nonRepetitionMapper A function applied to all messages which are not considered repetitions. Defaults
         *            to {@link UnaryOperator#identity()}.
         * @return The same builder with the non-repeated map operation set.
         */
        public Builder<T> setNonRepeatingMapper(UnaryOperator<T> nonRepetitionMapper) {
            this.nonRepetitionMapper = nonRepetitionMapper;
            return this;
        }

        /**
         * @param repetitionMapper A function applied to all messages which are considered repetitions. Defaults to
         *            {@link UnaryOperator#identity()}.
         * @return The same builder with the repeated map operation set.
         */
        public Builder<T> setRepeatingMapper(UnaryOperator<T> repetitionMapper) {
            this.repetitionMapper = repetitionMapper;
            return this;
        }

        /**
         * @param windowDuration The duration for how long the {@link RepetitionProcessor} processor looks for and
         *            groups repetitions. After each duration, messages are reset.
         * @return The same builder with the time-window duration set.
         * @throws IllegalArgumentException If the duration is <= 0.
         */
        public Builder<T> setWindowDuration(Duration windowDuration) {
            if (windowDuration.isNegative() || windowDuration.isZero()) {
                throw new IllegalArgumentException("Window duration cannot be less or equal to zero");
            }
            this.windowDuration = windowDuration;
            return this;
        }

    }
}

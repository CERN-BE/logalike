/**
 * Logalike - A stream based message processor Copyright (c) 2015 European Organisation for Nuclear Research (CERN), All
 * Rights Reserved. This software is distributed under the terms of the GNU General Public Licence version 3 (GPL
 * Version 3), copied verbatim in the file “COPYLEFT”. In applying this licence, CERN does not waive the privileges and
 * immunities granted to it by virtue of its status as an Intergovernmental Organization or submit itself to any
 * jurisdiction. Authors: Gergő Horányi <ghoranyi> and Jens Egholm Pedersen <jegp>
 */

package cern.acet.tracing.processing;

import java.time.Clock;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cern.acet.tracing.Message;
import cern.acet.tracing.processing.window.FingerprintStrategy;
import cern.acet.tracing.processing.window.MessageWindow;
import cern.acet.tracing.processing.window.WindowManager;

/**
 * <p>
 * A processor that throttles excessive input. The input is throttled whenever one emitter (defined by the
 * {@link FingerprintStrategy}, see below) sends more than a configurable number of messages per second (defaults to
 * 100). When that occurs, data from the emitter will be ignored in the interval defined in the constructor. Blocked
 * hosts that falls below the limit will be released. Every time a blocking or unblocking occurs, and every time a cycle
 * passed where a host remains blocked, a call will be sent to the {@link ThrottleListener}, where the listener can (but
 * doesn't have to) inform about the throttling.
 * </p>
 * <h2>Emitters</h2>
 * <p>
 * An emitter is a single source of messages, identified by a {@link FingerprintStrategy}, which can take a message and
 * generate a unique fingerprint from it. A typical example of an emitter is a host, process or similar.
 * </p>
 *
 * @param <T> The type of {@link Message} to process.
 * @author jepeders
 */
public class ThrottleProcessor<T extends Message<T>> implements Processor<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ThrottleProcessor.class);

    private final Map<String, ZonedDateTime> throttledEmitters = new ConcurrentHashMap<>();

    private final long messageLimitPerCycle;
    private final Duration throttleCycle;
    private final WindowManager<T> windowManager;
    private final FingerprintStrategy<T> fingerprintStrategy;
    private final ThrottleListener<T> listener;

    /**
     * Creates a throttling filter that uses an empty {@link ThrottleListener}, and does not warn about messages being
     * throttled.
     *
     * @param throttleCycle The duration of the cycle from which the messages are checked. Cannot be zero or less.
     * @param messageLimitPerCycle The limit of messages that an emitter can send per cycle.
     * @param fingerprintStrategy A strategy to uniquely identify message emitters.
     * @throws IllegalArgumentException If the cycle duration is zero or less.
     */
    public ThrottleProcessor(Duration throttleCycle, int messageLimitPerCycle,
            FingerprintStrategy<T> fingerprintStrategy) {
        this(throttleCycle, messageLimitPerCycle, fingerprintStrategy, new ThrottleListener<T>() {

            @Override
            public Optional<T> onThrottleStarting(ZonedDateTime startTime, String fingerprint, long count) {
                return Optional.empty();
            }

            @Override
            public Optional<T> onThrottleRecurring(ZonedDateTime startTime, String fingerprint, long count) {
                return Optional.empty();
            }

            @Override
            public Optional<T> onThrottleEnding(ZonedDateTime startTime, String fingerprint, long count) {
                return Optional.empty();
            }
        });
    }

    /**
     * Creates a throttling filter that uses the given {@link ThrottleListener} to inform whenever emitters are being
     * (un)throttled.
     *
     * @param throttleCycle The duration of the cycle from which the messages are checked. Cannot be zero or less.
     * @param messageLimitPerCycle The limit of messages that an emitter can send per cycle.
     * @param fingerprintStrategy A strategy to uniquely identify message emitters.
     * @param listener A {@link ThrottleListener} that can react to throttling events.
     * @throws IllegalArgumentException If the cycle duration is zero or less.
     */
    public ThrottleProcessor(Duration throttleCycle, int messageLimitPerCycle,
            FingerprintStrategy<T> fingerprintStrategy, ThrottleListener<T> listener) {
        if (throttleCycle.isZero() || throttleCycle.isNegative()) {
            throw new IllegalArgumentException("The throttle cycle cannot be zero or less");
        }

        this.fingerprintStrategy = fingerprintStrategy;
        this.listener = listener;
        this.messageLimitPerCycle = messageLimitPerCycle;
        this.throttleCycle = throttleCycle;
        this.windowManager = new WindowManager<T>(throttleCycle, fingerprintStrategy);

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(String.format("Throttle processor initialised with a cycle of %s and a message limit of %d",
                    throttleCycle.toString(), messageLimitPerCycle));
        }
    }

    /**
     * Processes the incoming stream by throttling incoming messages to the limits set by this class.
     *
     * @param stream The {@link Stream} to process.
     * @return A processed {@link Stream}.
     */
    @Override
    public Stream<T> apply(Stream<T> stream) {
        return apply(stream, () -> Clock.systemDefaultZone());
    }

    /**
     * Processes the incoming stream by throttling incoming messages to the limits set by this class. This method uses a
     * {@link Supplier} to calculate the time, which is useful for testing purposes.
     *
     * @param stream The {@link Stream} to process.
     * @param clockSupplier A {@link Supplier} that can produce {@link Clock}s which is used when closing messages.
     * @return A processed {@link Stream}.
     */
    Stream<T> apply(Stream<T> stream, Supplier<Clock> clockSupplier) {
        //@formatter:off
        Stream<T> messagesBelowThrottleLimit = stream.filter(message -> shouldThrottle(message, clockSupplier));

        Stream<T> throttledMessages = windowManager.getClosedWindowsStream()
                .map(decayed -> new ThrottleEntry(decayed, clockSupplier.get()))
                .map(message -> getThrottleMessage(message))
                .filter(option -> option.isPresent())
                .map(option -> option.get());
        //@formatter:on

        return Stream.concat(messagesBelowThrottleLimit, throttledMessages);
    }

    /**
     * @return The cycle of one throttle duration, which indicates how long emitters will be blocked.
     */
    public Duration getThrottleCycle() {
        return throttleCycle;
    }

    /**
     * Formats a throttle message from the given {@link ThrottleEntry}.
     *
     * @param entry The status to use as a source.
     * @return A {@link Message} with information drawn from the given window status.
     */
    private Optional<T> getThrottleMessage(ThrottleEntry entry) {
        switch (entry.status) {
        case STARTING:
            return listener.onThrottleStarting(entry.startTime, entry.fingerprint, entry.window.getCount());
        case RECURRING:
            return listener.onThrottleRecurring(entry.startTime, entry.fingerprint, entry.window.getCount());
        case ENDING:
            return listener.onThrottleEnding(entry.startTime, entry.fingerprint, entry.window.getCount());
        default:
            return Optional.empty();
        }
    }

    /**
     * Determines whether a message should be filtered out (throttled). A message is throttled if its emitter exists in
     * the list of throttled emitters or if the number of messages from the emitter is above the threshold.
     * 
     * @param message The message to examine.
     * @param clockSupplier A {@link Supplier} of {@link Clock}s for dependency injection.
     * @return True if the fingerprint is not in the list of throttled emitters and the number of messages from that
     *         emitter is below the threshold.
     */
    private boolean shouldThrottle(T message, Supplier<Clock> clockSupplier) {
        final String fingerprint = fingerprintStrategy.getFingerprint(message);
        /* Increment regardless of whether the message is blocked */
        final long newCounter = windowManager.increment(message, clockSupplier.get());
        return !throttledEmitters.containsKey(fingerprint) && newCounter <= messageLimitPerCycle;
    }

    /**
     * A listener that can react when the {@link ThrottleProcessor} activates events for certain message emitters. This
     * happens when a identified emitter (e. g. a host or whatever is identified by the {@link FingerprintStrategy})
     * sends too many messages, the {@link #onThrottleStarting(OffsetDateTime, Message, long)} messages will be called.
     * 
     * @author jepeders
     * @param <T> The type of {@link Message} the {@link ThrottleProcessor} treats.
     */
    public static interface ThrottleListener<T extends Message<T>> {

        /**
         * Informs the listener that an emitter is no longer throttled.
         * 
         * @param startTime The time when the emitter began to be throttled.
         * @param fingerprint A fingerprint of the emitter.
         * @param count The count of messages the emitter sent within the last duration.
         * @return A {@link Message} if the listener wishes to inform about the event, otherwise
         *         {@link Optional#empty()}.
         */
        public Optional<T> onThrottleEnding(ZonedDateTime startTime, String fingerprint, long count);

        /**
         * Informs the listener that an emitter has been and are still being blocked.
         * 
         * @param startTime The time when the emitter began to be throttled.
         * @param fingerprint A fingerprint of the emitter.
         * @param count The count of messages the emitter sent within the last duration.
         * @return A {@link Message} if the listener wishes to inform about the event, otherwise
         *         {@link Optional#empty()}.
         */
        public Optional<T> onThrottleRecurring(ZonedDateTime startTime, String fingerprint, long count);

        /**
         * Informs the listener that the emitter has not been blocked before, but will be blocked in the next cycle.
         * 
         * @param startTime The time when the emitter began to be throttled.
         * @param fingerprint A fingerprint of the emitter.
         * @param count The count of messages the emitter sent within the last duration.
         * @return A {@link Message} if the listener wishes to inform about the event, otherwise
         *         {@link Optional#empty()}.
         */
        public Optional<T> onThrottleStarting(ZonedDateTime startTime, String fingerprint, long count);

    }

    /**
     * A status of a message in the throttling filter.
     *
     * @author jepeders
     */
    enum ThrottleStatus {
        /**
         * If a message crossed the throttling threshold for the first time.
         */
        STARTING,

        /**
         * If a message is crossing the throttling threshold and crossed it previously.
         */
        RECURRING,

        /**
         * If a message that has previously been throttled, fell below the throttling threshold.
         */
        ENDING,

        /**
         * If a message is not throttled and is below the throttling threshold.
         */
        NORMAL;
    }

    /**
     * A data-type that contains information about the fingerprint and throttle status of a {@link MessageWindow}.
     *
     * @author jepeders
     */
    private class ThrottleEntry {

        private final ZonedDateTime startTime;
        private final String fingerprint;
        private final ThrottleStatus status;
        private final MessageWindow<T> window;

        /**
         * Creates a window status.
         *
         * @param window The window to store.
         * @param clock The clock to use when time-stamping the window.
         */
        private ThrottleEntry(MessageWindow<T> window, Clock clock) {
            this.window = window;
            this.fingerprint = fingerprintStrategy.getFingerprint(window.getMessage());
            this.status = updateEmitterStatus(window.getCount(), clock);
            this.startTime = Optional.ofNullable(throttledEmitters.get(fingerprint)).orElse(ZonedDateTime.now(clock));
        }

        /**
         * Updates the status of a emitter, evaluated based on the number of messages sent within a throttle cycle.
         *
         * @param count The number of messages sent within a throttle cycle.
         * @param fingerprint The name of the emitter.
         * @param clock the clock to use when storing the time-stamp for when an emitter is blocked for the first time.
         * @return A {@link ThrottleStatus}.
         */
        private synchronized ThrottleStatus updateEmitterStatus(long count, Clock clock) {
            final boolean isThrottled = throttledEmitters.containsKey(fingerprint);
            if (count > messageLimitPerCycle && isThrottled) {
                return ThrottleStatus.RECURRING;
            } else if (count > messageLimitPerCycle) {
                throttledEmitters.put(fingerprint, ZonedDateTime.now(clock));
                return ThrottleStatus.STARTING;
            } else if (isThrottled) {
                throttledEmitters.remove(fingerprint);
                return ThrottleStatus.ENDING;
            } else {
                return ThrottleStatus.NORMAL;
            }
        }

    }

}

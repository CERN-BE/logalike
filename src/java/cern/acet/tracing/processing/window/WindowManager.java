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

package cern.acet.tracing.processing.window;

import java.time.Clock;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import cern.acet.tracing.Message;

/**
 * Contains and manages time-based {@link MessageWindow}s that counts the number of occurrences for each message. The
 * {@link #increment(Message)} checks for the occurrence of the fingerprint of the given message and increments a
 * counter each time the same message is seen. The windows will automatically be closed if it is older than the decay
 * time, given in the constructor. The manager also schedules a clean-up of old {@link MessageWindow}s every time ten
 * window durations passes.
 *
 * @param <MessageType> The type of {@link Message} to manage.
 * @author jepeders
 */
public class WindowManager<MessageType extends Message<MessageType>> {

    private final LinkedBlockingQueue<MessageWindow<MessageType>> closedWindows = new LinkedBlockingQueue<MessageWindow<MessageType>>();
    private final Map<String, MessageWindow<MessageType>> messageWindows = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private final FingerprintStrategy<MessageType> strategy;
    private final Duration windowDuration;

    /**
     * Creates a manager for {@link MessageWindow}s.
     *
     * @param duration The duration of the windows.
     * @param strategy A function to determine a unique 'fingerprint' for a message.
     */
    public WindowManager(Duration duration, FingerprintStrategy<MessageType> strategy) {
        this.windowDuration = duration;
        this.strategy = strategy;

        /* Schedule closing of windows */
        scheduler.scheduleWithFixedDelay(this::closeWindows, windowDuration.toMillis(), windowDuration.toMillis(),
                TimeUnit.MILLISECONDS);
    }

    /**
     * Closes the currently open windows by removing them from the list of open windows and adding them to queue of
     * closed windows.
     * 
     * @param clock A clock to use for dependency injection.
     */
    private synchronized void closeWindows() {
        Map<String, MessageWindow<MessageType>> windowsToClose = new HashMap<>(messageWindows);
        windowsToClose.entrySet().stream().forEach(entry -> closeWindow(entry.getKey(), entry.getValue()));
    }

    /**
     * An infinite stream that outputs all message windows that has been closed and has a count of at least one.
     *
     * @return An infinite {@link Stream} of optional {@link Message}s and the number of duplicates found in the
     *         lifetime of the window.
     */
    public Stream<MessageWindow<MessageType>> getClosedWindowsStream() {
        return Stream.<MessageWindow<MessageType>> generate(() -> {
            try {
                return closedWindows.take();
            } catch (InterruptedException e) {
                throw new RuntimeException("Processing interrupted");
            }
        });
    }

    /**
     * Closes a window if it is older than the decay duration by removing it from the active windows. The window is put
     * in a list of decayed windows.
     *
     * @param fingerprint The fingerprint of the window.
     * @param window The window to decay, if it's old enough.
     */
    private synchronized void closeWindow(String fingerprint, MessageWindow<MessageType> window) {
        messageWindows.remove(fingerprint);
        closedWindows.add(window);
    }

    /**
     * Returns the window associated with the given host name.
     *
     * @param hostName The name to search for.
     * @return The window if it could be found.
     */
    public Optional<MessageWindow<MessageType>> getWindow(String hostName) {
        return Optional.ofNullable(messageWindows.get(hostName));
    }

    /**
     * Increments the counter for the given message, whose fingerprint is taken using the {@link FingerprintStrategy}
     * provided in the constructor.
     *
     * @param message The message whose counter to increment.
     * @return A positive integer denoting the current count of the message-window. 1 if the message has not been stored
     *         before.
     */
    public long increment(MessageType message) {
        return increment(message, Clock.systemDefaultZone());
    }

    /**
     * Increments the counter for the given message, whose fingerprint is taken using the {@link FingerprintStrategy}
     * provided in the constructor. The message is synchronized to avoid concurrent operations on the internal list of
     * message windows.
     *
     * @param message The message whose counter to increment.
     * @param clock The clock to use when examining if {@link MessageWindow}s have decayed.
     * @return A positive integer denoting the current count of the message-window. 1 if the message has not been stored
     *         before.
     */
    public synchronized long increment(MessageType message, Clock clock) {
        final String fingerprint = strategy.getFingerprint(message);
        final boolean contains = messageWindows.containsKey(fingerprint);
        if (!contains) {
            messageWindows.put(fingerprint, new MessageWindow<MessageType>(message.copy(), clock));
            return 1;
        } else {
            final MessageWindow<MessageType> messageWindow = messageWindows.get(fingerprint);
            return messageWindow.increment();
        }
    }
}

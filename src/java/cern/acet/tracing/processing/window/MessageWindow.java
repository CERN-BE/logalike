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
import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;

import cern.acet.tracing.Message;

/**
 * A message window which stores the time of creation and a count of how many times the window has been queried for
 * duplicates.
 * 
 * @param <MessageType> The type of {@link Message} stored in the {@link MessageWindow}.
 * @author jepeders
 */
public class MessageWindow<MessageType extends Message<MessageType>> {

    private final MessageType message;
    private final Instant startTime;
    private final AtomicLong counter = new AtomicLong(1);

    /**
     * Creates a window which started at the given time and a start count of 1.
     * 
     * @param message The message to store in the window. Also contains the timestamp for when the window started.
     * @param clock The clock to draw time from. Useful for testing.
     */
    public MessageWindow(MessageType message, Clock clock) {
        this.message = message;
        this.startTime = Instant.now(clock);
    }

    /**
     * Gets the original message that opened this {@link MessageWindow}.
     * 
     * @return A {@link Message}.
     */
    public MessageType getMessage() {
        return message;
    }

    /**
     * Gets the amount of times a duplicate of the underlying message has been seen.
     * 
     * @return A positive long.
     */
    public long getCount() {
        return counter.get();
    }

    /**
     * Gets the {@link Instant} when this window was created.
     * 
     * @return An {@link Instant}.
     */
    public Instant getStartTime() {
        return startTime;
    }

    /**
     * Increment the number of times a duplicate of the underlying message has been seen.
     * 
     * @return The new counter after incrementation.
     */
    public long increment() {
        return counter.incrementAndGet();
    }

    /**
     * Test if the given instance is older than the starting time of this window.
     * 
     * @param time The time to check.
     * @return True if the starting time was before this window was created.
     */
    public boolean isOlderThan(Instant time) {
        return startTime.isBefore(time);
    }

}
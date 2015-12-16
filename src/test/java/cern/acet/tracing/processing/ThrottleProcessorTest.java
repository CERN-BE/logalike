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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;

import cern.acet.tracing.MessageImpl;
import cern.acet.tracing.processing.ThrottleProcessor.ThrottleListener;

public class ThrottleProcessorTest {

    private static final String TEST_HOST = "testHost";
    private static final String HOST_FIELD = "host";
    private static final int MESSAGE_LIMIT = 1;
    private static final long DURATION_IN_MILLIS = 100;

    private static final Duration DURATION = Duration.ofMillis(DURATION_IN_MILLIS);
    private static final Clock CLOCK_NOW = getClock(0);

    private ThrottleListener<MessageImpl> mockListener;
    private Supplier<Clock> mockClockSupplier;
    private ThrottleProcessor<MessageImpl> filter;
    private MessageImpl message;
    private MessageImpl throttleMessage;

    @SuppressWarnings("unchecked")
    @Before
    public void setup() {
        message = MessageImpl.ofUntyped();
        throttleMessage = MessageImpl.ofUntyped();
        mockClockSupplier = mock(Supplier.class);
        mockListener = mock(ThrottleListener.class);
        when(mockListener.onThrottleEnding(Matchers.any(), Matchers.any(), Matchers.anyLong())).thenReturn(
                Optional.of(throttleMessage));
        when(mockListener.onThrottleRecurring(Matchers.any(), Matchers.any(), Matchers.anyLong())).thenReturn(
                Optional.of(throttleMessage));
        when(mockListener.onThrottleStarting(Matchers.any(), Matchers.any(), Matchers.anyLong())).thenReturn(
                Optional.of(throttleMessage));
        when(mockClockSupplier.get()).thenReturn(CLOCK_NOW);
        filter = new ThrottleProcessor<>(DURATION, MESSAGE_LIMIT, x -> x.getAsString(HOST_FIELD), mockListener);
        message.put(HOST_FIELD, TEST_HOST);
    }

    @Test
    public void canAllowMessagesWithNoHost() throws Exception {
        assertEquals(1, filterMessages(1).size());
    }

    @Test
    public void canUseClockSupplier() {
        filterMessages(1);
        verify(mockClockSupplier).get();
    }

    @Test
    public void canBlockEmitter() {
        assertEquals(1, filterMessages(2).stream().filter(m -> m.equals(message)).count());
    }

    @Test
    public void canSendBlockingMessage() {
        filterMessages(2);
        verify(mockListener).onThrottleStarting(ZonedDateTime.now(CLOCK_NOW), TEST_HOST, 2);
    }

    @Test
    public void canForwardThrottleMessageFromListener() {
        assertEquals(throttleMessage, filterStreamWithLimit(Stream.of(message, message), 2).get(1));
    }

    @Test
    public void canSendRecurringBlockingMessage() throws InterruptedException {
        filterMessages(2);
        Thread.sleep(DURATION_IN_MILLIS);
        filterStreamWithLimit(Stream.of(message, message), 1);
        verify(mockListener).onThrottleRecurring(ZonedDateTime.now(CLOCK_NOW), TEST_HOST, 2);
    }

    @Test
    public void canSendUnblockingMessage() throws InterruptedException {
        filterMessages(2);
        Thread.sleep(DURATION_IN_MILLIS);
        filterStreamWithLimit(Stream.of(message), 1);
        verify(mockListener).onThrottleEnding(ZonedDateTime.now(CLOCK_NOW), TEST_HOST, 1);
    }

    @Test
    public void canBlockHostInNextIteration() throws InterruptedException {
        filterMessages(2);
        Thread.sleep(DURATION_IN_MILLIS + 2);
        assertNotSame(message,
                filterStreamWithLimit(Stream.of(message, message, message.copy().put("host", "otherhost")), 2).get(0));
        verify(mockListener).onThrottleRecurring(ZonedDateTime.now(CLOCK_NOW), TEST_HOST, 2);
    }

    private List<MessageImpl> filterMessages(int number) {
        MessageImpl[] array = new MessageImpl[number];
        Arrays.fill(array, message);
        Stream<MessageImpl> stream = Stream.of(array);
        return filterStreamWithLimit(stream, number);
    }

    private List<MessageImpl> filterStreamWithLimit(Stream<MessageImpl> stream, int limit) {
        return filter.apply(stream, mockClockSupplier).limit(limit).collect(Collectors.toList());
    }

    private static Clock getClock(long ms) {
        return Clock.fixed(Instant.ofEpochMilli(ms), ZoneId.systemDefault());
    }

}

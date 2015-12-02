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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;

import org.junit.Before;
import org.junit.Test;

import cern.acet.tracing.MessageImpl;
import cern.acet.tracing.util.type.strategy.AcceptStrategy;

public class WindowManagerTest {

    private static final String FINGERPRINT_VALUE = "testValue";
    private static final String FINGERPRINT_FIELD = "body";
    private static final int DECAY_IN_MILLIS = 10;

    private static final FingerprintStrategy<MessageImpl> STRATEGY = message -> message.getOptionalAs(
            FINGERPRINT_FIELD, String.class).orElse("");
    private static final Duration DECAY = Duration.ofMillis(DECAY_IN_MILLIS);
    private static final MessageImpl MESSAGE = new MessageImpl(AcceptStrategy.INSTANCE).put(FINGERPRINT_FIELD,
            FINGERPRINT_VALUE);

    private static final Clock CLOCK_MINUS_DECAY = Clock.fixed(Instant.ofEpochMilli(100).minus(DECAY).minusMillis(1),
            ZoneId.systemDefault());

    private WindowManager<MessageImpl> manager;

    @Before
    public void setup() {
        manager = new WindowManager<>(DECAY, STRATEGY);
    }

    @Test
    public void canCreateManagerWithDefaultClock() throws Exception {
        manager = new WindowManager<>(DECAY, STRATEGY);
        manager.increment(MESSAGE);
        MessageWindow<MessageImpl> window = manager.getWindow(FINGERPRINT_VALUE).get();
        assertTrue(System.currentTimeMillis() - window.getStartTime().toEpochMilli() < 100);
    }

    @Test
    public void canSetFirstCounterToOne() {
        assertEquals(1, manager.increment(MESSAGE));
    }

    @Test
    public void canIncrementWindowCounter() throws Exception {
        manager.increment(MESSAGE);
        assertEquals(2, manager.increment(MESSAGE));
    }

    @Test
    public void canCloseWindowByScheduler() throws Exception {
        manager.increment(MESSAGE, CLOCK_MINUS_DECAY);
        assertTrue(manager.getClosedWindowsStream().findFirst().isPresent());
    }

    @Test
    public void canGetWindow() throws Exception {
        manager.increment(MESSAGE);
        assertTrue(manager.getWindow(FINGERPRINT_VALUE).isPresent());
    }

    @Test
    public void canGetEmptyWindow() throws Exception {
        assertFalse(manager.getWindow(FINGERPRINT_VALUE).isPresent());
    }

}

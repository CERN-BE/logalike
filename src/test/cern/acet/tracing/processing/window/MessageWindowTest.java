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
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;

import org.junit.Before;
import org.junit.Test;

import cern.acet.tracing.MessageImpl;

public class MessageWindowTest {

    private static final Clock CLOCK = Clock.fixed(Instant.now(), ZoneId.systemDefault());
    private MessageWindow<MessageImpl> window;
    private MessageImpl message;

    @Before
    public void setup() {
        message = new MessageImpl().putTimestamp(ZonedDateTime.now(CLOCK));
        window = new MessageWindow<>(message, CLOCK);
    }

    @Test
    public void canIncrement() {
        window.increment();
        assertEquals(2, window.getCount());
    }

    @Test
    public void canTestIfWindowIsOlder() throws Exception {
        assertTrue(window.isOlderThan(Instant.now(CLOCK).plusMillis(1)));
    }

    @Test
    public void canTestIfTimeIsYounger() throws Exception {
        assertFalse(window.isOlderThan(Instant.now(CLOCK).minusMillis(1)));
    }

    @Test
    public void canSetTimeFromMessage() throws Exception {
        ZonedDateTime instant = ZonedDateTime.now(CLOCK).plusDays(1);
        message = message.putTimestamp(instant);
        window = new MessageWindow<>(message, CLOCK);
        assertFalse(window.isOlderThan(Instant.now(CLOCK)));
    }

    @Test
    public void canSetTimeFromClock() throws Exception {
        message = new MessageImpl().remove(MessageImpl.TIMESTAMP_FIELD);
        window = new MessageWindow<>(message, CLOCK);
        assertFalse(window.isOlderThan(Instant.now(CLOCK)));
    }
}

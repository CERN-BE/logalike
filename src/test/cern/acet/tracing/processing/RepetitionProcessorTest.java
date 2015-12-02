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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Before;
import org.junit.Test;

import cern.acet.tracing.MessageImpl;

public class RepetitionProcessorTest {

    private static final String REPETITION_FIELD = "repeating";
    private static final String NON_REPEATING_VALUE = "nonRepeating";
    private static final String REPEATING_VALUE = "repeating";
    private static String FINGERPRINT_FIELD = "body";
    private static String FINGERPRINT_VALUE = "bodyValue";

    private static final Clock CLOCK_NOW = Clock.fixed(Instant.now(), ZoneId.systemDefault());
    private static final Duration WINDOW_DURATION = Duration.ofMillis(10);

    private static final UnaryOperator<MessageImpl> ACTION_REPEATING = message -> message.put(REPETITION_FIELD,
            REPEATING_VALUE);
    private static final UnaryOperator<MessageImpl> ACTION_NON_REPEATING = message -> message.put(REPETITION_FIELD,
            NON_REPEATING_VALUE);

    private MessageImpl message;

    private RepetitionProcessor<MessageImpl> filter;

    @Before
    public void setup() {
        filter = RepetitionProcessor.<MessageImpl> builder().setFingerprintStrategyByField(FINGERPRINT_FIELD)
                .setWindowDuration(WINDOW_DURATION).setRepeatingMapper(ACTION_REPEATING)
                .setNonRepeatingMapper(ACTION_NON_REPEATING).build();
        message = new MessageImpl().putTimestamp(ZonedDateTime.now(CLOCK_NOW))
                .put(FINGERPRINT_FIELD, FINGERPRINT_VALUE);
    }

    @Test
    public void canAllowMessageThrough() {
        assertTrue(filterSingle(message).isPresent());
    }

    @Test
    public void canAllowRepeatedMessageIfDuplicate() {
        assertTrue(filterMultiple(message, message).limit(2).allMatch(
                m -> m.get(FINGERPRINT_FIELD).equals(FINGERPRINT_VALUE)));
    }

    @Test
    public void canFilterMessageWithoutBodyField() {
        message = new MessageImpl();
        assertTrue(filterSingle(message).isPresent());
    }

    @Test
    public void canSetAggregatedIndex() {
        assertTrue(isRepeated(filterSingle(message).get()));
    }

    @Test
    public void canOutputMessageTwice() {
        List<MessageImpl> list = filterMultiple(message).limit(2).collect(Collectors.toList());
        assertTrue(isRepeated(list.get(0)));
        assertTrue(isAggregated(list.get(1)));
    }

    @Test
    public void canNotSetAggregatedIndexIfMessageIsDuplicated() {
        List<MessageImpl> filtered = filterMultiple(message, message).limit(3).collect(Collectors.toList());
        assertEquals(3, filtered.size());
        assertFalse(isAggregated(filtered.get(0)));
        assertFalse(isAggregated(filtered.get(1)));
        assertTrue(isAggregated(filtered.get(2)));
    }

    @Test
    public void canSetRepeatedIndex() {
        assertTrue(isRepeated(filterSingle(message).get()));
    }

    @Test
    public void canSetAggregatedIndexOnInfinite() {
        AtomicBoolean foundAggregated = new AtomicBoolean(false);
        try {
            filterInfinite(message).filter(m -> m.get(REPETITION_FIELD).equals(REPEATING_VALUE)).forEach(m -> {
                foundAggregated.set(true);
                throw new RuntimeException();
            });
        } catch (RuntimeException e) {
            /* Do nothing */
        }
        assertTrue(foundAggregated.get());
    }

    private Optional<MessageImpl> filterSingle(MessageImpl messageToFilter) {
        return filter.apply(Stream.of(messageToFilter)).findAny();
    }

    private Stream<MessageImpl> filterMultiple(MessageImpl... messagesToFilter) {
        return filter.apply(Stream.of(messagesToFilter));
    }

    private Stream<MessageImpl> filterInfinite(MessageImpl messageToRepeat) {
        return filter.apply(Stream.generate(() -> messageToRepeat)).parallel();
    }

    private boolean isAggregated(MessageImpl messageImpl) {
        return messageImpl.get(REPETITION_FIELD).equals(REPEATING_VALUE);
    }

    private boolean isRepeated(MessageImpl messageImpl) {
        return messageImpl.get(REPETITION_FIELD).equals(NON_REPEATING_VALUE);
    }

}

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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.util.function.Predicate;

import org.junit.Before;
import org.junit.Test;

import cern.acet.tracing.MessageImpl;

public class ConditionalMapperTest {

    private static final String TEST_KEY = "test";
    private static final int TEST_VALUE = 1;
    private static final Predicate<MessageImpl> PREDICATE_FALSE = (m) -> false;
    private static final Predicate<MessageImpl> PREDICATE_TRUE = (m) -> true;
    private MessageImpl mockMessage;

    @Before
    public void setup() {
        mockMessage = mock(MessageImpl.class);
    }

    @Test
    public void canCallActionWhenMatch() throws Exception {
        ConditionalMapper<MessageImpl> mapper = new ConditionalMapper<MessageImpl>(PREDICATE_TRUE, (m) -> m.put(
                TEST_KEY, TEST_VALUE));
        mapper.apply(mockMessage);
        verify(mockMessage).put(TEST_KEY, TEST_VALUE);
    }

    @Test
    public void canNotCallActionWhenMismatch() throws Exception {
        ConditionalMapper<MessageImpl> mapper = new ConditionalMapper<MessageImpl>(PREDICATE_FALSE, (m) -> m.put(
                TEST_KEY, TEST_VALUE));
        mapper.apply(mockMessage);
        verify(mockMessage, never()).put(TEST_KEY, TEST_VALUE);
    }
}

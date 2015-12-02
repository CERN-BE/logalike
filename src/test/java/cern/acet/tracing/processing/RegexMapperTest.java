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
import static org.mockito.Mockito.when;

import java.util.Optional;

import org.junit.Before;
import org.junit.Test;

import cern.acet.tracing.MessageImpl;

public class RegexMapperTest {

    private static final String TEST_KEY = "key";
    private static final String TEST_VALUE = "test";
    private MessageImpl mockMessage;

    @Before
    public void setup() {
        mockMessage = mock(MessageImpl.class);
        when(mockMessage.getOptionalAs(TEST_KEY, String.class)).thenReturn(Optional.of(TEST_VALUE));
    }

    @Test
    public void canCallActionOnFind() throws Exception {
        RegexMapper.<MessageImpl> ofFind(TEST_KEY, "te", m -> m.copy()).apply(mockMessage);
        verify(mockMessage).copy();
    }

    @Test
    public void canNotCallActionOnFindMismatch() throws Exception {
        RegexMapper.<MessageImpl> ofFind(TEST_KEY, "text", m -> m.copy()).apply(mockMessage);
        verify(mockMessage, never()).copy();
    }

    @Test
    public void canCallActionOnMatch() throws Exception {
        RegexMapper.<MessageImpl> ofMatch(TEST_KEY, "test", m -> m.copy()).apply(mockMessage);
        verify(mockMessage).copy();
    }

    @Test
    public void canNotCallActionOnPartialMatch() throws Exception {
        RegexMapper.<MessageImpl> ofMatch(TEST_KEY, "te", m -> m.copy()).apply(mockMessage);
        verify(mockMessage, never()).copy();
    }

}

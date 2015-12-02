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

package cern.acet.tracing.output;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import cern.acet.tracing.MessageImpl;
import cern.acet.tracing.Output;

public class LogalikeOutputTest {

    private Output<MessageImpl> output;
    private int messagesReceived;

    @Before
    public void setup() {
        output = new Output<MessageImpl>() {

            @Override
            public void close() throws IOException {
                /* Do nothing */
            }

            @Override
            public void accept(MessageImpl message) {
                messagesReceived++; 
            }};
    }

    @Test
    public void canForwardStreamAcceptCalls() {
        List<MessageImpl> messages = Arrays.asList(new MessageImpl(), new MessageImpl());
        messages.stream().forEach(output::accept);
        assertEquals(messages.size(), messagesReceived);
    }

}

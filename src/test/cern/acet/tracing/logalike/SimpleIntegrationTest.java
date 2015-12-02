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

package cern.acet.tracing.logalike;

import static org.junit.Assert.assertNotSame;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import org.junit.Before;
import org.junit.Test;

import cern.acet.tracing.Input;
import cern.acet.tracing.Logalike;
import cern.acet.tracing.LogalikeImpl;
import cern.acet.tracing.LogalikeImpl.Builder;
import cern.acet.tracing.MessageImpl;
import cern.acet.tracing.Output;
import cern.acet.tracing.processing.KeyValueMapper;
import cern.acet.tracing.processing.RepetitionProcessor;
import cern.acet.tracing.processing.ThrottleProcessor;

public class SimpleIntegrationTest {

    private static final String HOST_VALUE = "testHost";
    private static final String HOST_FIELD = "host";
    private static final String BODY_FIELD = "body";
    private Input<MessageImpl> input;
    private Output<MessageImpl> output;

    private KeyValueMapper<MessageImpl> kvMapper;
    private RepetitionProcessor<MessageImpl> duplicateProcessor;
    private ThrottleProcessor<MessageImpl> throttleProcessor;

    private AtomicInteger received;

    @Before
    public void setup() {
        input = new Input<MessageImpl>() {

            @Override
            public void close() throws IOException {
                /* Do nothing */
            }

            @Override
            public Stream<MessageImpl> get() {
                return Stream.generate(() -> new MessageImpl().put(HOST_FIELD, HOST_VALUE));
            }
        };

        received = new AtomicInteger();
        output = new Output<MessageImpl>() {

            @Override
            public void close() throws IOException {
                /* Do nothing */
            }

            @Override
            public void accept(MessageImpl message) {
                received.incrementAndGet();
            }
        };

        kvMapper = KeyValueMapper.<MessageImpl> builder().setFieldToParse(BODY_FIELD).build();
        duplicateProcessor = RepetitionProcessor.<MessageImpl> builder().setFingerprintStrategyByField(BODY_FIELD)
                .setWindowDuration(Duration.ofMillis(1)).build();
        throttleProcessor = new ThrottleProcessor<MessageImpl>(Duration.ofMillis(1), 10,
                message -> message.getAsString(HOST_FIELD));
    }

    @Test
    public void integrationTest() throws InterruptedException, IOException {
        Builder<MessageImpl> builder = Logalike.<MessageImpl> builder();
        builder.addMapper(kvMapper);
        builder.addProcessor(duplicateProcessor).addProcessor(throttleProcessor);
        builder.setInput(input);
        builder.setOutput(output);
        LogalikeImpl<MessageImpl> logalike = builder.build();
        Thread thread = new Thread(logalike);
        thread.start();

        thread.join(500);
        logalike.close();
        assertNotSame(0, received.get());
    }
}
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

package cern.acet.tracing.input.file;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.Test;

public class FileTailerListenerTest {

    private PositionFileTailerListener listener;
    private LinkedBlockingQueue<String> queue = new LinkedBlockingQueue<>(10);

    @Before
    public void setup() {
        listener = new PositionFileTailerListener(queue, Optional.empty());
    }

    @Test
    public void canReadLine() throws Exception {
        canRead("test");
    }

    @Test
    public void canReadSeveralLines() throws Exception {
        canRead("test", "test2", "test1232121");
    }

    private void canRead(String... lines) throws Exception {
        List<String> expected = Arrays.asList(lines);
        AtomicReference<List<String>> actual = new AtomicReference<>();
        Thread t = new Thread(() -> {
            try {
                Thread.sleep(50); // Wait for the tailer to read the lines
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            actual.set(queue.stream().limit(expected.size()).collect(Collectors.toList()));
        });
        t.start();
        expected.stream().forEach(listener::handle);
        t.join();
        assertEquals(expected, actual.get());
    }
}

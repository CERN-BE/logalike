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

package cern.acet.tracing.input.file.tailer;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.Mockito.*;

public class PositionTailerTest {

    private AtomicBoolean isInit = new AtomicBoolean(false);
    private PositionTailer.Builder tailerBuilder;
    private Thread thread;
    private Path file;
    private PositionTailerListener mockListener;
    private PositionTailer tailer;

    @Before
    public void setup() throws IOException, InterruptedException {
        mockListener = mock(PositionTailerListener.class);
        doAnswer(invocation -> {isInit.set(true); return null; })
                .when(mockListener)
                .init(Matchers.any(PositionTailer.class));
        file = Files.createTempFile("tailer", null);
        tailerBuilder = PositionTailer.builder().setFile(file.toFile()).setFileCheckInterval(Duration.ZERO)
                .setListener(mockListener);
    }

    @After
    public void cleanup() throws Exception {
        tailer.stop();
        thread.join();
        try {
            Files.delete(file);
        } catch (IOException e) {
            // Do nothing
        }
    }

    @Test
    public void canReadFromTail() throws Exception {
        start();
        String data = write("testString", file);
        Thread.sleep(20);
        verify(mockListener).handle(data);
    }

    @Test
    public void canReadFromSetPosition() throws Exception {
        tailerBuilder.setStartPosition(4);
        String data = write("testString", file);
        start();
        verify(mockListener).handle(data.substring(4));
    }

    @Test
    public void canReadFromEnd() throws Exception {
        tailerBuilder.setStartPositionAtEndOfFile();
        start();
        verify(mockListener, never()).handle(Matchers.anyString());
    }

    @Test
    public void canReadFromStart() throws Exception {
        String data = write("testString", file);
        tailerBuilder.setStartPositionAtBeginningOfFile();
        start();
        verify(mockListener).handle(data);
    }

    @Test
    public void canKeepReadingIfFileRotates() throws Exception {
        Path newFile = Paths.get(file.toString() + "moved");
        start();
        String data = write("testString", file);
        Files.move(file, newFile);
        Thread.sleep(10);
        String newData = write("testString2", file);
        verify(mockListener).handle(data);
        verify(mockListener).handle(newData);
        Files.delete(newFile);
    }

    private void start() throws InterruptedException {
        tailer = tailerBuilder.build();
        thread = new Thread(tailer, "Tailer testing");
        thread.start();
        // Wait for init
        while (!isInit.get()) {
            Thread.sleep(10);
        }
        Thread.sleep(20);
    }

    private String write(String data, Path file) throws IOException {
        Files.write(file, (data + '\n').getBytes());
        return data;
    }

}

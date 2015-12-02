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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.Duration;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.io.input.Tailer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;

public class FileTailerFactoryTest {

    private static final String DATA = "test";
    private static final Duration FILE_CHECK_INTERVAL = Duration.ofMillis(1);
    private FileTailerFactory factory;
    private ExecutorService executor;
    private File file;

    @Before
    public void setup() throws IOException {
        factory = new FileTailerFactory(FILE_CHECK_INTERVAL);
        file = File.createTempFile("logalike", null);
        executor = Executors.newSingleThreadExecutor();
    }

    @After
    public void teardown() throws Exception {
        executor.shutdown();
        factory.close();
    }

    @Test
    public void canStartAFileTailer() {
        Executor mockExecutor = mock(Executor.class);
        factory.startTailer(file, mockExecutor, true);
        verify(mockExecutor).execute(Matchers.any(Tailer.class));
    }

    @Test
    public void canStartMultipleFileTailers() throws IOException {
        File file2 = File.createTempFile("logalike", null);
        Executor mockExecutor = mock(Executor.class);
        factory.startTailer(file, mockExecutor, true);
        factory.startTailer(file2, mockExecutor, true);
        verify(mockExecutor, times(2)).execute(Matchers.any(Tailer.class));
    }

    @Test
    public void canCreateAFileTailerFromBeginningOfFile() throws IOException {
        writeToFile(DATA);
        factory.startTailer(file, executor, true);
        assertEquals(DATA, factory.getStream().findAny().get());
    }

    @Test
    public void canCreateAFileTailerFromEndOfFile() throws IOException, InterruptedException {
        writeToFile("wrongData");
        factory.startTailer(file, executor, false);
        Thread.sleep(100);
        writeToFile(DATA);
        assertEquals(DATA, factory.getStream().findAny().get());
    }

    private void writeToFile(String data) throws IOException {
        try (FileOutputStream fileOut = new FileOutputStream(file); PrintWriter writer = new PrintWriter(fileOut)) {
            writer.println(data);
        }
    }
}

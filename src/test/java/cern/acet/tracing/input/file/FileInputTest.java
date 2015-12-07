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
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import cern.acet.tracing.MessageImpl;

public class FileInputTest {

    private static final File INPUT_SAMPLE = new File(ClassLoader.getSystemClassLoader().getResource("sample-input.txt")
            .getFile());

    private FileInput<MessageImpl> fileInput;
    private File dataFile;

    @Before
    public void setup() throws IOException {
        dataFile = File.createTempFile("logalike", "syslog");
    }

    @After
    public void teardown() throws Exception {
        fileInput.close();
        dataFile.delete();
    }

    @Test
    public void canReadAFile() {
        fileInput = createInput(dataFile);
        populateWithDelay(INPUT_SAMPLE, dataFile, 10);
        MessageImpl message = fileInput.get().findAny().get();
        assertTrue(message.containsKey("body"));
    }

    @Test
    public void canReadMultipleFiles() throws Exception {
        File dataFile2 = File.createTempFile("logalike", "syslog");
        fileInput = createInput(Arrays.asList(dataFile, dataFile2));
        /* Make sure the inputs have been created */
        Thread.sleep(200);

        populateWithDelay(INPUT_SAMPLE, dataFile, 100);
        populateWithDelay(INPUT_SAMPLE, dataFile2, 100);
        assertEquals(20, fileInput.get().limit(20).count());
        dataFile2.delete();
    }

    @Test
    public void canReadGlobbedFiles() {
        fileInput = createInput(dataFile.toString().substring(0, dataFile.toString().length()) + "*");
        populateWithDelay(INPUT_SAMPLE, dataFile, 10);
        assertTrue(fileInput.get().findAny().get().containsKey("body"));
    }

    private FileInput<MessageImpl> createInput(File file) {
        return FileInput.buildTailing(line -> {
            return new MessageImpl().put("body", line);
        }).addFile(file).build();
    }

    private FileInput<MessageImpl> createInput(List<File> files) {
        final TailingBuilder<MessageImpl> builder = FileInput.buildTailing(line -> {
            return new MessageImpl().put("body", line);
        });
        files.forEach(builder::addFile);
        return builder.build();
    }

    private FileInput<MessageImpl> createInput(String glob) {
        return FileInput.buildTailing(line -> {
            return new MessageImpl().put("body", line);
        }).addFiles(glob).build();
    }

    private void populateWithDelay(File inputFile, File outputFile, int delay) {
        Thread thread = new Thread(() -> {
            try (BufferedReader input = new BufferedReader(new FileReader(inputFile));
                    PrintStream output = new PrintStream(outputFile)) {

                while (true) {
                    String line = input.readLine();
                    if (line == null) {
                        break;
                    }
                    output.println(line);
                    Thread.sleep(delay);
                }
            } catch (Exception e) {
                System.err.println(e);
            }
        });
        thread.start();
    }
}

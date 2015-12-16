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

package cern.acet.tracing.input.file.store;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

import static org.junit.Assert.assertEquals;

public class FilePositionStoreTest {

    private FilePositionStore store;
    private Path parentPath;
    private Path filePath;

    @Before
    public void setup() throws IOException {
        parentPath = Files.createTempDirectory("store");
        filePath = Files.createTempFile("storefile", null);
        store = FilePositionStore.createUnder(parentPath);
    }

    @After
    public void cleanup() throws IOException {
        try {
            Files.list(parentPath).forEach(path -> {
                try {
                    Files.delete(path);
                } catch (IOException e) {
                    /* Do nothing */
                }
            });
            Files.delete(parentPath);
            Files.delete(filePath);
        } catch (IOException e) {
            /* Do nothing */
        }
    }

    @Test
    public void canReadNonExistentStoreEntry() throws IOException {
        assertEquals(Optional.empty(), store.getFilePosition(filePath));
    }

    @Test
    public void canSetStoreEntryWithoutException() {
        store.setFilePosition(filePath, 123);
    }

    @Test
    public void canReadPreviouslySetStorePosition() {
        Long data = 1312L;
        store.setFilePosition(filePath, data);
        assertEquals(Optional.of(data), store.getFilePosition(filePath));
    }

    @Test
    public void canRecoverPreviousStore() throws Exception {
        Long data = 1312L;
        store.setFilePosition(filePath, data);
        store.close();
        store = FilePositionStore.createUnder(parentPath);
        assertEquals(Optional.of(data), store.getFilePosition(filePath));
    }

    @Test(expected = IllegalArgumentException.class)
    public void canFailIfDirectoryIsFile() throws IOException {
        Files.delete(parentPath);
        Files.createFile(parentPath);
        store = FilePositionStore.createUnder(parentPath);
    }

    @Test
    public void canReturnEmptyPositionIfFileIsMoved() throws IOException {
        store.setFilePosition(filePath, 0L);
        Path newFile = Paths.get(filePath.toString() + "moved");
        Files.move(filePath, newFile);
        assertEquals(Optional.empty(), store.getFilePosition(filePath));
        Files.delete(newFile);
    }

    @Test
    public void canResetPositionIfFileIsMoved() throws IOException {
        store.setFilePosition(filePath, 0L);
        Path newFile = Paths.get(filePath.toString() + "moved");
        Files.move(filePath, newFile);
        Files.createFile(filePath);
        assertEquals(Optional.of(0L), store.getFilePosition(filePath));
        Files.delete(newFile);
    }

}

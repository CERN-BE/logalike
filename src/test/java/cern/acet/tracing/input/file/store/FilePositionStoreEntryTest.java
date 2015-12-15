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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class FilePositionStoreEntryTest {

    private FilePositionStoreEntry entry;
    private Path parentPath;
    private Path filePath;

    @Before
    public void setup() throws IOException {
        parentPath = Files.createTempDirectory("store");
        filePath = Files.createTempFile("storefile", null);
        entry = FilePositionStoreEntry.createEntry(parentPath, filePath.toAbsolutePath());
    }

    @After
    public void cleanup() throws IOException {
        Files.list(parentPath).forEach(path -> {
            try {
                Files.delete(path);
            } catch (IOException e) {
                /* Do nothing */
            }
        });
        Files.delete(parentPath);
        Files.delete(filePath);
    }

    @Test
    public void canCreateStoreEntry() {
        assertTrue(Files.exists(getStoreFile()));
    }

    @Test
    public void canReadStoreEntryFromFile() throws IOException {
        Long data = 23L;
        Files.write(getStoreFile(), FilePositionStoreEntry.longToBytes(data).array());
        assertEquals(Optional.of(data), entry.getFilePosition());
    }

    @Test
    public void canWriteStoreEntry() throws Exception {
        Long position = 3213L;
        entry.setFilePosition(position);
        assertEquals(Optional.of(position), entry.getFilePosition());
    }

    @Test
    public void canReReadPositionAfterClosing() throws Exception {
        Long position = 3123L;
        entry.setFilePosition(position);
        entry.close();
        entry = FilePositionStoreEntry.createEntry(parentPath, filePath.toAbsolutePath());
        assertEquals(Optional.of(position), entry.getFilePosition());
    }

    @Test
    public void canTestThatFileWasMoved() throws IOException, InterruptedException {
        Path newFile = Paths.get(filePath.toString() + "moved");
        Files.move(filePath, newFile);
        Thread.sleep(1000); // Some file timestamps is granular to seconds, so we'll have to wait to distinguish
        Files.createFile(filePath);
        assertFalse(entry.isSameFile(filePath));
    }

    private Path getStoreFile() {
        return parentPath.resolve(FilePositionStoreEntry.hashFileName(filePath.toAbsolutePath().toString()));
    }

}

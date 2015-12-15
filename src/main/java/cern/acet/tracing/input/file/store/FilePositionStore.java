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

import cern.acet.tracing.util.ThrowingConsumer;
import cern.acet.tracing.util.ThrowingSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Keeps track of file-pointers in files by storing them on disk. This is useful for later if a program
 * crashes or simply stops reading from a file, and you'd like to continue where you left off. By default the store
 * keeps its files under the user home directory, but that can be changed by giving the
 */
public class FilePositionStore implements AutoCloseable {

    public static final String DEFAULT_DIRECTORY_NAME = ".logalike_store";
    public static final String DEFAULT_DIRECTORY = System.getProperty("user.home") + File.separator +
            DEFAULT_DIRECTORY_NAME;

    private static final Logger LOGGER = LoggerFactory.getLogger(FilePositionStore.class);

    private final Map<Path, FilePositionStoreEntry> entries = new ConcurrentHashMap<>();
    private final Path directory;

    /**
     * Creates a {@link FilePositionStore} using the given directory as the parent for the store files. The directory
     * may not exists, but it cannot exist <b>and</b> not be a directory, i. e. a file or a link.
     *
     * @param directory The {@link Path} to the directory where to save files used by this store.
     * @throws IOException If the directory for the store could not be created or if it already exists, but is not
     *                     a directory.
     */
    private FilePositionStore(Path directory) throws IOException {
        this.directory = directory;
    }

    @Override
    public synchronized void close() throws Exception {
        Map<Path, FilePositionStoreEntry> entriesCopy = new HashMap<>(entries);
        entries.clear();
        entriesCopy.forEach((path, entry)-> {
            try {
                entry.close();
            } catch (Exception e) {
                LOGGER.debug("Failed to close file position entry for {}", path, e);
            }
        });
    }

    /**
     * Creates a {@link FilePositionStore} using the default directory name ({@value DEFAULT_DIRECTORY_NAME}) under
     * the current user directory.
     *
     * @return A {@link FilePositionStore} created under the user directory.
     * @throws IOException If the directory for the store could not be created or if it already exists, but is not
     *                     a directory.
     */
    public static FilePositionStore createUnderDefaultDirectory() throws IOException {
        return createUnder(Paths.get(DEFAULT_DIRECTORY));
    }

    /**
     * Creates a {@link FilePositionStore} storing the store files under the given directory.
     *
     * @param directory The parent directory where the file position store saves its file-pointers.
     * @return A {@link FilePositionStore} created under the user directory.
     * @throws IOException If the directory for the store did not exist but could not be created..
     * @throws IllegalArgumentException If the given directory is not a directory.
     */
    public static FilePositionStore createUnder(Path directory) throws IOException {
        if (!Files.exists(directory)) {
            Files.createDirectory(directory);
        } else if (!Files.isDirectory(directory)) {
            throw new IllegalArgumentException("The path to the store must be a directory: " + directory.toString());
        }
        return new FilePositionStore(directory);
    }

    /**
     * Reads the last know file-pointer position where the given file was read.
     *
     * @param file The file in question.
     * @return A long if it exists in the store.
     */
    public synchronized Optional<Long> getFilePosition(Path file) {
        return getFilePositionEntry(file).flatMap(entry -> tryGet(entry::getFilePosition));
    }

    private synchronized Optional<FilePositionStoreEntry> getFilePositionEntry(Path file) {
        final FilePositionStoreEntry mapEntry = entries.get(file);
        try {
            if (mapEntry != null && mapEntry.isSameFile(file)) {
                return Optional.of(mapEntry);
            }
        } catch (IOException e) {
            LOGGER.warn("Failed to get ");
        }

        Optional<FilePositionStoreEntry> newEntryOptional = createEntry(file);
        newEntryOptional.ifPresent(newEntry -> entries.put(file, newEntry));
        return newEntryOptional;
    }

    private Optional<FilePositionStoreEntry> createEntry(Path file) {
        try {
            return Optional.of(FilePositionStoreEntry.createEntry(directory, file.toAbsolutePath()));
        } catch (IOException e) {
            LOGGER.warn("Failed to read file position for " + file, e);
        }
        return Optional.empty();
    }

    /**
     * Updates the file-pointer in the given file.
     * @param file The file which should be updated.
     * @param position The position that was last read in the file.
     */
    public synchronized void setFilePosition(Path file, long position) {
        getFilePositionEntry(file).ifPresent(entry -> trySet(entry::setFilePosition, position));
    }

    private static <T> Optional<T> tryGet(ThrowingSupplier<Optional<T>, IOException> f) {
        try {
            return f.get();
        } catch (Exception e) {
            LOGGER.warn("Error when reading store position", e);
            return Optional.empty();
        }
    }

    private static <T> void trySet(ThrowingConsumer<T, IOException> f, T value) {
        try {
            f.accept(value);
        } catch (Exception e) {
            LOGGER.warn("Error when setting store position", e);
        }
    }
}

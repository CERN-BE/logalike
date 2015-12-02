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
import cern.acet.tracing.util.ThrowingFunction;
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
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Stores positions in files in a directory
 */
public class FilePositionStore {

    public static final String DEFAULT_DIRECTORY_NAME = ".logalike_store";
    public static final String DEFAULT_DIRECTORY = System.getProperty("user.home") + File.separator + DEFAULT_DIRECTORY_NAME;

    private static final Logger LOGGER = LoggerFactory.getLogger(FilePositionStore.class);

    private final int ERROR_VALUE = -1;

    private final Map<Path, FilePositionStoreEntry> entries = new ConcurrentHashMap<>();
    private final Path directory;

    /**
     * Creates a {@link FilePositionStore} using the default directory name ({@value DEFAULT_DIRECTORY_NAME}) under
     * the current user directory.
     *
     * @throws IOException If the directory for the store could not be created or if it already exists, but is not
     *                     a directory.
     */
    public FilePositionStore() throws IOException {
        this(Paths.get(DEFAULT_DIRECTORY));
    }

    /**
     * Creates a {@link FilePositionStore} using the given directory. The directory cannot exist and <b>not</b> be
     * a directory, i. e. a file or a link.
     *
     * @param directory The {@link Path} to the directory where to save files used by this store.
     * @throws IOException If the directory for the store could not be created or if it already exists, but is not
     *                     a directory.
     */
    public FilePositionStore(Path directory) throws IOException {
        this.directory = Files.createDirectory(directory);
    }

    public synchronized Optional<Long> getFilePosition(Path file) {
        return getFilePositionEntry(file).flatMap(entry -> tryGet(entry::getFilePosition));
    }

    public synchronized Optional<FilePositionStoreEntry> getFilePositionEntry(Path file) {
        final FilePositionStoreEntry oldEntry = entries.get(file);
        if (oldEntry == null) {
            Optional<FilePositionStoreEntry> newEntryOptional = createEntry(file);
            newEntryOptional.ifPresent(newEntry -> entries.put(file, newEntry));
            return newEntryOptional;
        } else {
            return Optional.of(oldEntry);
        }
    }

    private Optional<FilePositionStoreEntry> createEntry(Path file) {
        try {
            return Optional.of(new FilePositionStoreEntry(directory, file));
        } catch (IOException e) {
            LOGGER.warn("Failed to create file for " + file, e);
        }
        return Optional.empty();
    }

    public synchronized void setFilePosition(Path file, long position) {
        getFilePositionEntry(file).ifPresent(entry -> trySet(entry::setFilePosition, position));
    }

    private static <T> Optional<T> tryGet(ThrowingSupplier<T, IOException> f) {
        try {
            return Optional.of(f.get());
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

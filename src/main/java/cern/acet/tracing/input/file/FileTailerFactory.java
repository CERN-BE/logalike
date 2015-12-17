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

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import cern.acet.tracing.input.file.store.FilePositionStore;
import cern.acet.tracing.input.file.tailer.PositionTailer;
import org.apache.commons.io.input.Tailer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cern.acet.tracing.util.StreamUtils;

/**
 * A factory for creating a single {@link FileInput} which collect lines from one or many {@link File}.
 */
public class FileTailerFactory implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(FileTailerFactory.class);
    private static final int QUEUE_CAPACITY = 500;

    private final Optional<FilePositionStore> positionStoreOption;
    private final Duration fileCheckInterval;
    private final AtomicBoolean isOpen = new AtomicBoolean(true);
    private final LinkedBlockingQueue<String> lineQueue = new LinkedBlockingQueue<>(QUEUE_CAPACITY);

    /**
     * Creates a {@link FileTailerFactory} that spawns {@link Tailer}s which check for file changes in the given
     * interval.
     * 
     * @param fileCheckInterval How often files should be checked for changes.
     */
    public FileTailerFactory(Duration fileCheckInterval) {
        this.fileCheckInterval = fileCheckInterval;

        FilePositionStore tempPositionStore;
        try {
            tempPositionStore = FilePositionStore.createUnderDefaultDirectory();
        } catch (IOException e) {
            LOGGER.warn("Failed to create file position store", e);
            tempPositionStore = null;
        }
        positionStoreOption = Optional.ofNullable(tempPositionStore);
    }

    @Override
    public void close() throws Exception {
        isOpen.set(false);
        positionStoreOption.ifPresent((filePositionStore) -> {
            try {
                filePositionStore.close();
            } catch (Exception e) {
                LOGGER.warn("Failed to close file position store", e);
            }
        });
    }

    public Stream<String> getStream() {
        return StreamUtils.takeWhile(Stream.generate(() -> {
            try {
                return lineQueue.take();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }), message -> isOpen.get());
    }

    /**
     * Creates a {@link Tailer} and starts its execution in the given {@link Executor}.
     *
     * @param file The file to tail.
     * @param executor The executor to run the {@link Tailer} in.
     * @param readFromBeginning True if the tailer should read the file from the beginning, false if the tailer should
     *            only read new lines from the end of the file.
     * @return The started tailer.
     */
    public PositionTailer startTailer(File file, Executor executor, boolean readFromBeginning) {
        PositionFileTailerListener listener = new PositionFileTailerListener(lineQueue, positionStoreOption);
        PositionTailer.Builder builder = PositionTailer.builder()
                .setFile(file)
                .setListener(listener)
                .setFileCheckInterval(fileCheckInterval);

        if (readFromBeginning) {
            builder.setStartPositionAtBeginningOfFile();
        } else {
            // Set the starting position if it's available in the position store
            positionStoreOption.flatMap(store -> store.getFilePosition(file.toPath())).map(builder::setStartPosition);
        }

        final PositionTailer tailer = builder.build();
        executor.execute(tailer);

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Started tailing file " + file);
        }

        return tailer;
    }

}

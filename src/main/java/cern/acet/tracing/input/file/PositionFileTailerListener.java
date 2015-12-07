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
import java.util.Optional;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import cern.acet.tracing.input.file.store.FilePositionStore;
import org.apache.commons.io.input.Tailer;
import org.apache.commons.io.input.TailerListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of a {@link TailerListener} that can used by a {@link Tailer}. The
 * {@link PositionFileTailerListener} uses a {@link LinkedBlockingQueue} to compress the lines coming
 * from the {@link Tailer}s into a single {@link Stream} of lines ({@link String}s).
 *
 * @author jepeders
 */
public class PositionFileTailerListener implements PositionTailerListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(PositionFileTailerListener.class);
    private static final int TIMEOUT = 1;
    private static final TimeUnit TIMEOUT_UNIT = TimeUnit.MINUTES;

    private final LinkedBlockingQueue<String> queue;
    private final Optional<FilePositionStore> positionStoreOption;
    private File tailingFile;

    /**
     * Creates a {@link PositionFileTailerListener} which uses the given queue to enqueue lines.
     * 
     * @param queue The queue to use to enqueue lines.
     * @param positionStore A FilePositionStore to update the file positions for future recovery if the tailer stops.
     */
    public PositionFileTailerListener(LinkedBlockingQueue<String> queue, Optional<FilePositionStore> positionStore) {
        this.queue = queue;
        this.positionStoreOption = positionStore;
    }

    @Override
    public void init(PositionTailer tailer) {
        this.tailingFile = tailer.getFile();
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Tailer {} starting to read from file {}", tailer, tailingFile);
        }
    }

    @Override
    public void fileNotFound() {
        LOGGER.warn("File not found: {}", tailingFile);
    }

    @Override
    public void fileRotated() {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Rotated {}", tailingFile);
        }
    }

    @Override
    public synchronized void handle(String line) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("Read a line from {}: {}", tailingFile, line);
        }

        try {
            queue.offer(line, TIMEOUT, TIMEOUT_UNIT);
        } catch (InterruptedException e) {
            LOGGER.warn("Failed to enqueue line after {} {} from {}:", TIMEOUT, TIMEOUT_UNIT, tailingFile, e);
        }


    }

    @Override
    public void handle(Exception exception) {
        LOGGER.warn("Exception when tailing file {}", tailingFile, exception);
    }

    @Override
    public void positionUpdated(long position) {
        positionStoreOption.ifPresent(store -> store.setFilePosition(tailingFile.toPath(), position));
    }

}

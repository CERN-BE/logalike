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
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.apache.commons.io.input.Tailer;
import org.apache.commons.io.input.TailerListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of a {@link TailerListener} that can used by a {@link Tailer}. The {@link FileTailerListener} uses
 * a {@link LinkedBlockingQueue} to compress the lines coming from the {@link Tailer}s into a single {@link Stream} of
 * lines ({@link String}s).
 *
 * @author jepeders
 */
public class FileTailerListener implements TailerListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(FileTailerListener.class);
    private static final int TIMEOUT = 1;
    private static final TimeUnit TIMEOUT_UNIT = TimeUnit.MINUTES;

    private final LinkedBlockingQueue<String> queue;
    private File tailingFile;

    /**
     * Creates a {@link FileTailerListener} which uses the given queue to enqueue lines.
     * 
     * @param queue The queue to use to enqueue lines.
     */
    public FileTailerListener(LinkedBlockingQueue<String> queue) {
        this.queue = queue;
    }

    @Override
    public void init(Tailer tailer) {
        this.tailingFile = tailer.getFile();
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Tailer initialised {} using file {}", tailer, tailingFile);
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

}

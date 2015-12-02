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
import java.time.Duration;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import org.apache.commons.io.input.Tailer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cern.acet.tracing.util.StreamUtils;

public class FileTailerFactory implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(FileTailerFactory.class);
    private static final int QUEUE_CAPACITY = 500;

    private final Duration fileCheckInterval;
    private final AtomicBoolean isOpen = new AtomicBoolean(true);
    private final LinkedBlockingQueue<String> lineQueue = new LinkedBlockingQueue<String>(QUEUE_CAPACITY);

    /**
     * Creates a {@link FileTailerFactory} that spawns {@link Tailer}s which check for file changes in the given
     * interval.
     * 
     * @param fileCheckInterval How often files should be checked for changes.
     */
    public FileTailerFactory(Duration fileCheckInterval) {
        this.fileCheckInterval = fileCheckInterval;
    }

    @Override
    public void close() throws Exception {
        isOpen.set(false);
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
    public Tailer startTailer(File file, Executor executor, boolean readFromBeginning) {
        FileTailerListener listener = new FileTailerListener(lineQueue);
        Tailer tailer = new Tailer(file, listener, fileCheckInterval.toMillis(), !readFromBeginning);
        executor.execute(tailer);

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Started tailer for file " + file);
        }

        return tailer;
    }

}

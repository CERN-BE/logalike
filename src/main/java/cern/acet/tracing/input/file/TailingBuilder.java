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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import cern.acet.tracing.input.file.tailer.PositionTailer;
import org.apache.commons.io.input.Tailer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cern.acet.tracing.Message;

/**
 * <p>
 * A {@link FileInputBuilder} that constructs a {@link FileInput} that continuously checks for added content to one or
 * more files.
 * </p>
 * <p>
 * By default the {@link TailingBuilder} only tails the files, that is only reading new lines that will be added to the
 * files, starting from the time when the {@link #build()} method is called. It can be set to start at the beginning of
 * the file instead by using {@link #shouldReadFromBeginning(boolean)}.
 * </p>
 * <p>
 * Files are checked for changes every second by default. This can be changed via the
 * {@link #setFileCheckInterval(Duration)}.
 * </p>
 *
 * @author jepeders
 * @param <MessageType> The type of {@link Message} converted from the output of the file(s).
 * @see PositionTailer
 * @see cern.acet.tracing.input.file.store.FilePositionStore
 */
public class TailingBuilder<MessageType extends Message<MessageType>> implements
        FileInputBuilder<MessageType, TailingBuilder<MessageType>> {

    private static final Duration DEFAULT_FILE_CHECK_INTERVAL = Duration.ofMillis(500);
    private static final Logger LOGGER = LoggerFactory.getLogger(FileInput.class);

    private final Function<String, MessageType> converter;
    private final List<File> files = new ArrayList<>();

    private Duration fileCheckInterval = DEFAULT_FILE_CHECK_INTERVAL;
    private boolean isReadingFromBeginning = false;
    private FileTailerFactory tailerFactory;
    private List<PositionTailer> tailers;

    /**
     * Creates a {@link TailingBuilder} that uses the given converter to parse {@link String}s into {@link Message}s.
     *
     * @param converter The converter to use when converting file input to {@link Message}s.
     */
    public TailingBuilder(Function<String, MessageType> converter) {
        this.converter = converter;
    }

    @Override
    public TailingBuilder<MessageType> addFile(File file) {
        files.add(verifyCanRead(file));
        return this;
    }

    @Override
    public TailingBuilder<MessageType> addFiles(String glob) {
        files.addAll(getFilesFromGlob(glob));
        return this;
    }

    @Override
    public FileInput<MessageType> build() {
        if (files.isEmpty()) {
            throw new IllegalStateException(
                    "No files were added to the builder. Cannot construct tailer from zero input.");
        }

        tailerFactory = new FileTailerFactory(fileCheckInterval);
        final ExecutorService executor = Executors.newScheduledThreadPool(files.size());
        this.tailers = files.stream().map(file -> tailerFactory.startTailer(file, executor, isReadingFromBeginning))
                .collect(Collectors.toList());

        LOGGER.info("Created file input tailing {} file(s): {}", files.size(), files);
        return new FileInput<>(this, executor);
    }

    @Override
    public AutoCloseable getCloseableHook() {
        return new AutoCloseable() {

            /* Copy the resource to allow the TailingBuilder to be garbage collected */
            private final List<PositionTailer> tailersToClose = tailers;

            @Override
            public void close() throws Exception {
                tailersToClose.forEach(PositionTailer::stop);
                tailerFactory.close();
            }
        };
    }

    @Override
    public Stream<MessageType> getStream() {
        return tailerFactory.getStream().map(converter);
    }

    /**
     * Sets the interval with which the {@link FileInput} should check for new data in the tailing files.
     *
     * @param interval The interval to check for changes in the files.
     * @return A {@link TailingBuilder} with the interval set.
     */
    public TailingBuilder<MessageType> setFileCheckInterval(Duration interval) {
        if (interval.isZero() || interval.isNegative()) {
            throw new IllegalArgumentException("Interval cannot be zero or below");
        }
        this.fileCheckInterval = interval;
        return this;
    }

    /**
     * Defines whether the {@link FileInput} should read from the beginning of a file or not. Defaults to false.
     *
     * @param shouldReadFromBeginning If set to true, all files will be read from the beginning. If false, the
     *            {@link FileInput} will wait for content to be added to the file.
     * @return A {@link TailingBuilder} with the <code>isReadingFromBeginning</code> flag set.
     */
    public TailingBuilder<MessageType> shouldReadFromBeginning(boolean shouldReadFromBeginning) {
        this.isReadingFromBeginning = shouldReadFromBeginning;
        return this;
    }

}
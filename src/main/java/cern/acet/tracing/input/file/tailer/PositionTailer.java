package cern.acet.tracing.input.file.tailer;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import cern.acet.tracing.input.file.PositionFileTailerListener;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.time.Duration;

/**
 * Slightly modified implementation of the unix "tail -f" functionality from the Apache commons library.
 * This version exposes the file position to the {@link PositionTailerListener}
 * <p>
 * <h2>1. Create a PositionTailerListener implementation</h3>
 * <p>
 * First you need to create a {@link PositionTailerListener} implementation.
 * </p>
 *
 * <p>For example:</p>
 * <pre>
 *  public class MyTailerListener extends PositionTailerListenerAdapter {
 *      public void handle(String line) {
 *          System.out.println(line);
 *      }
 *  }
 * </pre>
 *
 * <h2>2. Using a PositionTailer</h2>
 *
 * You can create and use a PositionTailer by constructing an instance via the Builder and runnning it, preferably
 * by using an executor.
 *
 * <pre>
 *      PositionTailer tailer = ...;
 *
 *      // stupid executor impl. for demo purposes
 *      Executor executor = new Executor() {
 *          public void execute(Runnable command) {
 *              command.run();
 *           }
 *      };
 *
 *      executor.execute(tailer);
 * </pre>
 *
 * <h2>3. Stop Tailing</h3>
 * <p>Remember to stop the tailer when you have done with it:</p>
 * <pre>
 *      tailer.stop();
 * </pre>
 *
 * @see PositionTailerListener
 */
public class PositionTailer implements Runnable {

    private static final String RAF_MODE = "r";

    /**
     * Buffer on top of RandomAccessFile.
     */
    private final byte inbuf[];

    /**
     * The file which will be tailed.
     */
    private final File file;

    /**
     * The amount of time to wait for the file to be updated.
     */
    private final long delayMillis;

    /**
     * The listener to notify of events when tailing.
     */
    private final PositionTailerListener listener;

    /**
     * Whether to close and reopen the file whilst waiting for more input.
     */
    private final boolean reOpen;

    /**
     * The position where to start reading from the file. If null, the file is read from the end.
     */
    private final Long startingPosition;

    /**
     * The tailer will run as long as this value is true.
     */
    private volatile boolean run = true;

    /**
     * Creates a PositionTailer for the given file, with a specified buffer size.
     * @param builder The builder to use when constructing the PositionTailer
     */
    private PositionTailer(Builder builder) {
        this.file = builder.file;
        this.delayMillis = builder.delayMillis;
        this.startingPosition = builder.startingPosition;

        this.inbuf = new byte[builder.bufSize];

        // Save and prepare the listener
        this.listener = builder.listener;
        listener.init(this);
        this.reOpen = builder.reOpen;
    }

    /**
     * @return A new Builder which can help construct instances of the PositionTailer.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Return the file.
     *
     * @return the file
     */
    public File getFile() {
        return file;
    }

    /**
     * Return the delay in milliseconds.
     *
     * @return the delay in milliseconds.
     */
    public long getDelay() {
        return delayMillis;
    }

    /**
     * Follows changes in the file, calling the PositionTailerListener's handle method for each new line.
     */
    public void run() {
        RandomAccessFile reader = null;
        try {
            long last = 0; // The last time the file was checked for changes
            long position = 0; // position within the file
            // Open the file
            while (run && reader == null) {
                try {
                    reader = new RandomAccessFile(file, RAF_MODE);
                } catch (FileNotFoundException e) {
                    listener.fileNotFound();
                }

                if (reader == null) {
                    try {
                        Thread.sleep(delayMillis);
                    } catch (InterruptedException e) {
                    }
                } else {
                    // The current position in the file
                    position = startingPosition == null ? file.length() : startingPosition;
                    last = System.currentTimeMillis();
                    reader.seek(position);
                }
            }

            while (run) {

                boolean newer = FileUtils.isFileNewer(file, last); // IO-279, must be done first

                // Check the file length to see if it was rotated
                long length = file.length();

                if (length < position) {

                    // File was rotated
                    listener.fileRotated();

                    // Reopen the reader after rotation
                    try {
                        // Ensure that the old file is closed iff we re-open it successfully
                        RandomAccessFile save = reader;
                        reader = new RandomAccessFile(file, RAF_MODE);
                        position = 0;
                        // close old file explicitly rather than relying on GC picking up previous RAF
                        IOUtils.closeQuietly(save);
                    } catch (FileNotFoundException e) {
                        // in this case we continue to use the previous reader and position values
                        listener.fileNotFound();
                    }
                    continue;
                } else {

                    // File was not rotated

                    // See if the file needs to be read again
                    if (length > position) {

                        // The file has more content than it did last time
                        position = readLines(reader);
                        last = System.currentTimeMillis();

                    } else if (newer) {

                        /*
                         * This can happen if the file is truncated or overwritten with the exact same length of
                         * information. In cases like this, the file position needs to be reset
                         */
                        position = 0;
                        reader.seek(position); // cannot be null here

                        // Now we can read new lines
                        position = readLines(reader);
                        last = System.currentTimeMillis();
                    }
                    listener.positionUpdated(position);

                }
                if (reOpen) {
                    IOUtils.closeQuietly(reader);
                }
                try {
                    Thread.sleep(delayMillis);
                } catch (InterruptedException e) {
                }
                if (run && reOpen) {
                    reader = new RandomAccessFile(file, RAF_MODE);
                    reader.seek(position);
                }
            }

        } catch (Exception e) {

            listener.handle(e);

        } finally {
            IOUtils.closeQuietly(reader);
        }
    }

    /**
     * Allows the tailer to complete its current loop and return.
     */
    public void stop() {
        this.run = false;
    }

    /**
     * Read new lines.
     *
     * @param reader The file to read
     * @return The new position after the lines have been read
     * @throws java.io.IOException if an I/O error occurs.
     */
    private long readLines(RandomAccessFile reader) throws IOException {
        StringBuilder sb = new StringBuilder();

        long pos = reader.getFilePointer();
        long rePos = pos; // position to re-read

        int num;
        boolean seenCR = false;
        while (run && ((num = reader.read(inbuf)) != -1)) {
            for (int i = 0; i < num; i++) {
                byte ch = inbuf[i];
                switch (ch) {
                    case '\n':
                        seenCR = false; // swallow CR before LF
                        listener.handle(sb.toString());
                        sb.setLength(0);
                        rePos = pos + i + 1;
                        break;
                    case '\r':
                        if (seenCR) {
                            sb.append('\r');
                        }
                        seenCR = true;
                        break;
                    default:
                        if (seenCR) {
                            seenCR = false; // swallow final CR
                            listener.handle(sb.toString());
                            sb.setLength(0);
                            rePos = pos + i + 1;
                        }
                        sb.append((char) ch); // add character, not its ascii value
                }
            }

            pos = reader.getFilePointer();
        }

        reader.seek(rePos); // Ensure we can re-read if necessary
        return rePos;
    }

    /**
     * A builder which can help to construct a PositionTailer.
     */
    public static class Builder {
        private int bufSize = 4096;
        private long delayMillis = 1000;
        private File file;
        private PositionTailerListener listener;
        private boolean reOpen = false;
        private Long startingPosition;

        /**
         * Attempts to build an instance of a PositionTailer with the current parameters.
         * @return A PositionTailer.
         * @throws IllegalArgumentException If the file or listener is not set.
         */
        public PositionTailer build() {
            if (file == null) {
                throw new IllegalArgumentException("File must be set");
            }
            if (listener == null) {
                throw new IllegalArgumentException("Listener must be set");
            }
            return new PositionTailer(this);
        }

        /**
         * @param bufferSize The size of the byte-buffer to read data into. Defaults to 4096.
         * @return The same builder with the given buffer size.
         */
        public Builder setBufferSize(int bufferSize) {
            if (bufferSize < 1) {
                throw new IllegalArgumentException("Cannot set a buffer size to less than 1");
            }
            this.bufSize = bufferSize;
            return this;
        }

        /**
         * Sets the time-interval between checks for new content in the file. Defaults to 1 second.
         * @param fileCheckInterval The interval with which the PositionTailer will check for new content.
         * @return The same builder with the file check interval set.
         */
        public Builder setFileCheckInterval(Duration fileCheckInterval) {
            if (fileCheckInterval == null || fileCheckInterval.isNegative()) {
                throw new IllegalArgumentException("Cannot set file check interval to less than 0");
            }
            this.delayMillis = fileCheckInterval.toMillis();
            return this;
        }

        /**
         * @param file The File from which the PositionTailer should read. Required field.
         * @return The same builder with the file defined.
         */
        public Builder setFile(File file) {
            this.file = file;
            return this;
        }

        /**
         * @param listener The PositionFileTailerListener which catches events from the built PositionTailers.
         *                 This field is required.
         * @return The same builder with the listener set.
         */
        public Builder setListener(PositionFileTailerListener listener) {
            this.listener = listener;
            return this;
        }

        /**
         * Configures the PositionTailer to close and re-open the file after each read.
         * @param reOpen A boolean value whether to close and re-open (true) or not (false).
         * @return The same Builder with a flag to re-open the file set,
         */
        public Builder setReOpenAfterRead(boolean reOpen) {
            this.reOpen = reOpen;
            return this;
        }

        /**
         * Configures the PositionTailer to start reading from the given position. Cannot be less than 0.
         * @param position The position to start reading from. If set to zero, the file will be read from the beginning.
         * @return The same Builder with the starting position set.
         */
        public Builder setStartPosition(long position) {
            if (position < 0) {
                throw new IllegalArgumentException("Cannot set position to less than 0");
            }
            this.startingPosition = position;
            return this;
        }

        /**
         * Configures the PositionTailer to start reading at the beginning of the file.
         * @return The same Builder with the starting position set to the beginning of the file.
         */
        public Builder setStartPositionAtBeginningOfFile() {
            this.startingPosition = 0L;
            return this;
        }

        /**
         * Configures the PositionTailer to start reading at the end of the file. This is the default setting.
         * @return The same Builder with the starting position set to the end of the file.
         */
        public Builder setStartPositionAtEndOfFile() {
            this.startingPosition = null;
            return this;
        }

    }

}

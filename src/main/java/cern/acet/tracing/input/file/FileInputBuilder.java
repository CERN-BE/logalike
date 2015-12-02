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
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cern.acet.tracing.Message;

/**
 * A builder for {@link FileInput}s.
 *
 * @author jepeders
 * @param <MessageType> The type of {@link Message} converted from the output of the file(s).
 * @param <BuilderType> The sub-type of the builder to return on builder operations.
 */
public interface FileInputBuilder<MessageType extends Message<MessageType>, BuilderType extends FileInputBuilder<MessageType, BuilderType>> {

    Pattern GROK_PARENT_PATTERN = Pattern.compile("^([^?*]*\\" + File.separator + ")(.*)$");
    Logger LOGGER = LoggerFactory.getLogger(FileInputBuilder.class);

    /**
     * Adds a file that the {@link FileInput} will read when built.
     *
     * @param file The file to read.
     * @return A {@link FileInputBuilder} with the file set.
     * @throws IllegalArgumentException If the file did not exist or could not be read.
     */
    BuilderType addFile(File file) throws IllegalArgumentException;

    /**
     * Adds one or more files using a glob pattern, as described in {@link FileSystem#getPathMatcher(String)}. However
     * only glob patterns are supported, so the input string should not be appended with '<code>glob:</code>'.
     *
     * @param glob The glob pattern to read. The pattern should not be appended with '<code>glob:</code>'.
     * @return A {@link FileInputBuilder} with the file set.
     * @see FileSystem#getPathMatcher(String).
     * @throws IllegalArgumentException If one or more of the found files did not exist or could not be read.
     */
    BuilderType addFiles(String glob) throws IllegalArgumentException;

    /**
     * Builds the {@link FileInput}.
     *
     * @return A {@link FileInput}.
     * @throws IllegalStateException If the builder is built without any files.
     */
    FileInput<MessageType> build() throws IllegalStateException;

    /**
     * @return A hook for closing the resources started by the {@link FileInputBuilder}.
     */
    AutoCloseable getCloseableHook();

    /**
     * Gets the stream for the {@link FileInputBuilder}.
     *
     * @return A {@link Stream} of {@link Message}s.
     */
    Stream<MessageType> getStream();

    /**
     * Verifies that a file exists and can be read.
     *
     * @param file The file to verify.
     * @return A {@link File} that exists and can be read.
     * @throws IllegalArgumentException if the file does not exist or cannot be read.
     */
    default File verifyCanRead(File file) {
        if (file.canRead()) {
            return file;
        } else {
            throw new IllegalArgumentException("Cannot read file: " + file);
        }
    }

    /**
     * Retrieves a list of files from a glob pattern.
     *
     * @param glob The glob pattern.
     * @return A {@link List} of {@link File}.
     */
    default List<File> getFilesFromGlob(String glob) {
        List<File> files = new ArrayList<>();
        PathMatcher pathMatcher = FileSystems.getDefault().getPathMatcher("glob:" + glob);
        Path prefixPath = getGlobPrefixPath(glob);
        try {
            Files.walkFileTree(prefixPath, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    if (pathMatcher.matches(file)) {
                        files.add(file.toFile());
                    }
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFileFailed(Path file, IOException ex) throws IOException {
                    LOGGER.debug("Visit to {} failed", file, ex);
                    return FileVisitResult.CONTINUE;
                }
            });
        } catch (IOException e) {
            LOGGER.warn("Omitting glob '{}':", glob, e);
        }
        return files;
    }

    /**
     * Gets the glob prefix, defined as the last directory that is not globbed. This is done to avoid traversing as many
     * files as possible.
     *
     * <pre>
     *   /tmp/dir/*-glob/log -> /tmp/dir/
     *   /tmp/dir?/log       -> /tmp/
     * </pre>
     *
     * @param glob The glob to find the prefix in.
     * @return The lowest path in the glob-pattern that is not globbed.
     */
    default Path getGlobPrefixPath(String glob) {
        Matcher matcher = GROK_PARENT_PATTERN.matcher(glob);
        if (matcher.matches()) {
            return Paths.get(matcher.group(1));
        } else {
            return Paths.get("");
        }
    }

}
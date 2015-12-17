package cern.acet.tracing.input.file.store;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.charset.Charset;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Instant;
import java.util.Optional;

import static com.google.common.hash.Hashing.*;
import static java.nio.file.StandardOpenOption.*;

/**
 * An entry in the {@link FilePositionStore} which accounts for one single file. When the entry is created it tries
 * to open a file and lock it for the duration of the entry. It is therefore very important that the entry is
 * {@link #close()}d when the entry is no longer used.
 */
class FilePositionStoreEntry implements AutoCloseable {

    private static final int BEGINNING_OF_FILE = 0;

    private final Instant timeOfCreation;
    private final FileChannel channel;
    private final FileLock lock;

    /**
     * Creates a new {@link FilePositionStoreEntry} which uses the given channel to write file positions to.
     * @param channel The channel to read from.
     * @param lock The file lock which will be held for the duration of the class.
     * @param timeOfCreation The {@link Instant} when the file we are tracking was created. Used to check for log
     *                       rotations.
     * @throws IOException If the file could not be read or we failed to lock the store file (already in use?).
     */
    FilePositionStoreEntry(FileChannel channel, FileLock lock, Instant timeOfCreation) throws IOException{
        this.channel = channel;
        this.timeOfCreation = timeOfCreation;
        this.lock = lock;
    }

    @Override
    public void close() throws Exception {
        lock.release();
        channel.close();
    }

    /**
     * Creates a {@link FilePositionStoreEntry} under the given directory, which uses the given file-name to uniquely
     * identify any previously stored file positions for that file.
     * @param parentDirectory The parent directory for the file position store.
     * @param file The file to track. Should be the absolute name of the file avoid name-clashes.
     * @return A {@link FilePositionStoreEntry}.
     * @throws IOException If the file could not be opened.
     */
    public static FilePositionStoreEntry createEntry(Path parentDirectory, Path file) throws IOException {
        final String encoded = hashFileName(file.toString());
        final Path storeFile = Paths.get(parentDirectory.toString(), encoded);
        if (!Files.exists(storeFile)) {
            Files.createFile(storeFile);
        }

        try {
            final FileChannel channel = FileChannel.open(storeFile, SYNC, READ, WRITE);
            final FileLock lock = channel.tryLock();
            return new FilePositionStoreEntry(channel, lock, getTimeOfCreation(file));
        } catch (OverlappingFileLockException e) {
            throw new IOException(e);
        }
    }

    /**
     * Verifies that the given file is the same that we are currently tracking by comparing the time of creation for the
     * file we are currently tracking and the given file.
     *
     * @param file The other file to compare with the currently tracked file.
     * @return True if the file from this entry is the same as the given file.
     * @throws IOException If we could not read the file attributes of the given file.
     */
    public boolean isSameFile(Path file) throws IOException {
        Instant newFileTime = getTimeOfCreation(file);
        return timeOfCreation.compareTo(newFileTime) == 0;
    }

    private static Instant getTimeOfCreation(Path file) throws IOException {
        BasicFileAttributes attributes = Files.readAttributes(file, BasicFileAttributes.class);
        return attributes.creationTime().toInstant();
    }

    public Optional<Long> getFilePosition() throws IOException {
        channel.position(0);
        return readLongFromChannel(channel);
    }

    /**
     * Creates a unique name based on the absolute path of the given file.
     * @param fileName The file name to hash.
     * @return A hashed String of the absolute file path.
     */
    public static String hashFileName(String fileName) {
        return md5().hashString(fileName, Charset.defaultCharset()).toString();
    }

    public void setFilePosition(long filePosition) throws IOException {
        channel.write(longToBytes(filePosition), BEGINNING_OF_FILE);
    }

    static ByteBuffer longToBytes(long number) {
        // Thanks to http://stackoverflow.com/a/4485196/999865
        final ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(number);
        buffer.flip();
        return buffer;
    }

    static Optional<Long> readLongFromChannel(ReadableByteChannel channel) throws IOException {
        // Thanks to http://stackoverflow.com/a/4485196/999865
        final ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        channel.read(buffer);
        if (buffer.position() == 0) {
            return Optional.empty();
        } else {
            buffer.flip();
            return Optional.of(buffer.getLong());
        }
    }

}
package cern.acet.tracing.input.file.store;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.*;
import java.util.Base64;

/**
 * An entry in the {@link FilePositionStore} which accounts for one single file. When the entry is created it tries
 * to open a file and lock it for the duration of the entry. It is therefore very important that the entry is
 * {@link #close()}d when the entry is no longer used.
 */
public class FilePositionStoreEntry implements AutoCloseable {

    private final FileChannel channel;
    private final FileLock lock;

    public FilePositionStoreEntry(Path parentDirectory, Path file) throws IOException {
        final String encoded = new String(Base64.getEncoder().encode(file.toString().getBytes()));
        final Path storeFile = Files.createFile(Paths.get(parentDirectory.toString(), encoded));
        channel = FileChannel.open(storeFile, StandardOpenOption.SYNC);

        try {
            lock = channel.tryLock();
        } catch (OverlappingFileLockException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void close() throws Exception {
        lock.release();
    }

    public long getFilePosition() throws IOException {
        channel.position(0);
        return readFromChannel(channel);
    }

    public synchronized void setFilePosition(long filePosition) throws IOException {
        channel.position(0);
        channel.write(longToBytes(filePosition));
    }

    private synchronized static ByteBuffer longToBytes(long number) {
        // Thanks to http://stackoverflow.com/a/4485196/999865
        final ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(number);
        return buffer;
    }

    private synchronized static long readFromChannel(FileChannel channel) throws IOException {
        // Thanks to http://stackoverflow.com/a/4485196/999865
        final ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        channel.read(buffer);
        buffer.flip();
        return buffer.getLong();
    }
}

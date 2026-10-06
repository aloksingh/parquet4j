package io.github.aloksingh.parquet;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.concurrent.Executors;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class PageFileChunkReaderTest {
    @TempDir
    Path tempDir;

    @Test
    void rejectsInvalidRangesBeforeAllocating() throws IOException {
        Path path = tempDir.resolve("ranges.bin");
        Files.write(path, new byte[]{1, 2, 3, 4});
        try (FileChunkReader reader = new FileChunkReader(path)) {
            assertThrows(IllegalArgumentException.class, () -> reader.readBytes(0, -1));
            assertThrows(IllegalArgumentException.class, () -> reader.readBytes(-1, 1));
            assertThrows(IllegalArgumentException.class, () -> reader.readBytes(Long.MAX_VALUE, 1));
            assertThrows(IOException.class, () -> reader.readBytes(5, 1));
            assertEquals(2, reader.readBytes(2, 20).remaining(), "readBytes clamps at EOF");
            assertEquals(0, reader.readBytes(4, 0).remaining());
        }
    }

    @Test
    void rejectsReadsAfterCloseIncludingZeroLength() throws IOException {
        Path path = tempDir.resolve("closed.bin");
        Files.write(path, new byte[]{1});
        FileChunkReader reader = new FileChunkReader(path);
        reader.close();
        assertThrows(IOException.class, () -> reader.readBytes(0, 0));
        assertThrows(IOException.class, reader::length);
        reader.close();
    }

    @Test
    void completesConcurrentPositionalReadsWithoutSharingAFileCursor() throws Exception {
        byte[] expected = new byte[128 * 1024 + 13];
        for (int i = 0; i < expected.length; i++) expected[i] = (byte) (i * 13);
        Path path = tempDir.resolve("concurrent.bin");
        Files.write(path, expected);
        try (FileChunkReader reader = new FileChunkReader(path);
             var executor = Executors.newFixedThreadPool(8)) {
            var results = new ArrayList<java.util.concurrent.Future<ByteBuffer>>();
            for (int i = 0; i < 32; i++) {
                int offset = i * 17;
                results.add(executor.submit(() -> reader.readBytes(offset, expected.length - offset)));
            }
            for (int i = 0; i < results.size(); i++) {
                ByteBuffer result = results.get(i).get();
                byte[] actual = new byte[result.remaining()];
                result.get(actual);
                assertArrayEquals(java.util.Arrays.copyOfRange(expected, i * 17, expected.length), actual);
            }
        }
    }
}

package io.github.aloksingh.parquet;

import io.github.aloksingh.parquet.model.ColumnDescriptor;
import io.github.aloksingh.parquet.model.Encoding;
import io.github.aloksingh.parquet.model.Page;
import io.github.aloksingh.parquet.model.Type;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Format-byte fixtures independent of the production writer.
 */
final class DecodingTestSupport {
    private DecodingTestSupport() {
    }

    static ColumnDescriptor descriptor(Type type, int definitions, int repetitions) {
        return new ColumnDescriptor(type, new String[]{"col"}, definitions, repetitions,
                type == Type.FIXED_LEN_BYTE_ARRAY ? 3 : 0);
    }

    static ByteBuffer levels(int maxLevel, int... values) {
        if (maxLevel == 0) {
            return ByteBuffer.allocate(0);
        }
        int width = 32 - Integer.numberOfLeadingZeros(maxLevel);
        int bytes = (width + 7) / 8;
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        for (int value : values) {
            output.write(2); // One-value RLE run, not a production encoder round trip.
            for (int i = 0; i < bytes; i++) {
                output.write(value >>> (8 * i));
            }
        }
        return ByteBuffer.wrap(output.toByteArray());
    }

    static Page page(boolean v2, ColumnDescriptor descriptor, Encoding encoding,
                     int[] definitions, int[] repetitions, ByteBuffer values) {
        int count = definitions.length;
        ByteBuffer defs = levels(descriptor.maxDefinitionLevel(), definitions);
        ByteBuffer reps = levels(descriptor.maxRepetitionLevel(), repetitions);
        if (v2) {
            int nulls = 0;
            int rows = 0;
            for (int i = 0; i < count; i++) {
                if (definitions[i] < descriptor.maxDefinitionLevel()) nulls++;
                if (repetitions[i] == 0) rows++;
            }
            return new Page.DataPageV2(values, count, nulls, rows, encoding, defs, reps, false);
        }
        int repBytes = reps.hasRemaining() ? reps.remaining() + 4 : 0;
        int defBytes = defs.hasRemaining() ? defs.remaining() + 4 : 0;
        ByteBuffer data = ByteBuffer.allocate(repBytes + defBytes + values.remaining())
                .order(ByteOrder.LITTLE_ENDIAN);
        if (repBytes > 0) data.putInt(reps.remaining()).put(reps);
        if (defBytes > 0) data.putInt(defs.remaining()).put(defs);
        data.put(values.duplicate()).flip();
        return new Page.DataPage(data, count, encoding, defBytes, repBytes);
    }

    static ByteBuffer consecutiveDelta(long first, long step, int count) {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        unsigned(output, 128);
        unsigned(output, 4);
        unsigned(output, count);
        unsigned(output, (first << 1) ^ (first >> 63));
        if (count > 1) {
            unsigned(output, (step << 1) ^ (step >> 63));
            for (int i = 0; i < 4; i++) output.write(0);
        }
        return ByteBuffer.wrap(output.toByteArray());
    }

    static ByteBuffer delta(int... values) {
        if (values.length == 0 || values.length > 129) throw new AssertionError("Fixture size");
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        unsigned(output, 128);
        unsigned(output, 4);
        unsigned(output, values.length);
        unsigned(output, ((long) values[0] << 1) ^ (values[0] >> 31));
        if (values.length == 1) return ByteBuffer.wrap(output.toByteArray());
        long minimum = Long.MAX_VALUE;
        for (int i = 1; i < values.length; i++) minimum = Math.min(minimum, (long) values[i] - values[i - 1]);
        unsigned(output, (minimum << 1) ^ (minimum >> 63));
        int[] widths = new int[4];
        for (int mini = 0; mini < 4; mini++) {
            long maximum = 0;
            for (int i = 1 + mini * 32; i < values.length && i < 1 + (mini + 1) * 32; i++) {
                maximum = Math.max(maximum, (long) values[i] - values[i - 1] - minimum);
            }
            widths[mini] = maximum == 0 ? 0 : 64 - Long.numberOfLeadingZeros(maximum);
            output.write(widths[mini]);
        }
        for (int mini = 0; mini < 4 && 1 + mini * 32 < values.length; mini++) {
            byte[] packed = new byte[4 * widths[mini]];
            for (int item = 0; item < 32 && 1 + mini * 32 + item < values.length; item++) {
                int index = 1 + mini * 32 + item;
                long adjusted = (long) values[index] - values[index - 1] - minimum;
                for (int bit = 0; bit < widths[mini]; bit++) {
                    if ((adjusted & (1L << bit)) != 0) {
                        int offset = item * widths[mini] + bit;
                        packed[offset / 8] |= (byte) (1 << (offset % 8));
                    }
                }
            }
            output.writeBytes(packed);
        }
        return ByteBuffer.wrap(output.toByteArray());
    }

    static void unsigned(ByteArrayOutputStream output, long value) {
        while ((value & ~0x7fL) != 0) {
            output.write((int) ((value & 0x7f) | 0x80));
            value >>>= 7;
        }
        output.write((int) value);
    }

    static ByteBuffer plain(Type type, Object... values) {
        ByteBuffer buffer = ByteBuffer.allocate(4096).order(ByteOrder.LITTLE_ENDIAN);
        if (type == Type.BOOLEAN) {
            for (int start = 0; start < values.length; start += 8) {
                int bits = 0;
                for (int bit = 0; bit < 8 && start + bit < values.length; bit++) {
                    if ((Boolean) values[start + bit]) bits |= 1 << bit;
                }
                buffer.put((byte) bits);
            }
        } else {
            for (Object value : values) {
                switch (type) {
                    case INT32 -> buffer.putInt((Integer) value);
                    case INT64 -> buffer.putLong((Long) value);
                    case FLOAT -> buffer.putFloat((Float) value);
                    case DOUBLE -> buffer.putDouble((Double) value);
                    case BYTE_ARRAY -> buffer.putInt(((byte[]) value).length).put((byte[]) value);
                    case FIXED_LEN_BYTE_ARRAY, INT96 -> buffer.put((byte[]) value);
                    default -> throw new AssertionError(type);
                }
            }
        }
        return buffer.flip();
    }
}

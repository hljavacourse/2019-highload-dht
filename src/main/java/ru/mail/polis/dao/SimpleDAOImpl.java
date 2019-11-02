package ru.mail.polis.dao;

import org.jetbrains.annotations.NotNull;
import org.rocksdb.BuiltinComparator;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import ru.mail.polis.Record;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class SimpleDAOImpl implements DAO {

    private final RocksDB rocksDB;

    public SimpleDAOImpl(@NotNull final RocksDB db) {
        rocksDB = db;
    }

    /**
     * create of SimpleDAOImpl.
     *
     * @param data file
     * @return new SimpleDAOImpl()
     * @throws IOException when catch RocksDBException
     */
    public static SimpleDAOImpl init(final File data) throws IOException {
        try {
            final Options options = new Options();
            options.setComparator(BuiltinComparator.BYTEWISE_COMPARATOR);
            options.setCreateIfMissing(true);
            final RocksDB rocksDB = RocksDB.open(options, data.getAbsolutePath());

            return new SimpleDAOImpl(rocksDB);
        } catch (RocksDBException exp) {
            throw new IOException(exp);
        }
    }

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull final ByteBuffer from) throws IOException {
        final RocksIterator iterator = rocksDB.newIterator();
        iterator.seek(getArrayByteMinusMinValue(from));

        return new RocksDBIterator(iterator);
    }

    @NotNull
    @Override
    public ByteBuffer get(@NotNull final ByteBuffer key) throws IOException, NoSuchElementException {
        try {
            final byte[] value = rocksDB.get(getArrayByteMinusMinValue(key));
            if (value == null) {
                throw new NoSuchElementLite("Key is not present: " + key.toString());
            }
            return ByteBuffer.wrap(value);
        } catch (RocksDBException exp) {
            throw new IOException(exp);
        }
    }

    /**
     * method for get with Timestamp.
     *
     * @param keys ByteBuffer keys
     * @return Timestamp
     * @throws IOException            may be throw IOException
     * @throws NoSuchElementException may be throw NoSuchElementException
     */
    public Timestamp getWithTimestamp(@NotNull final ByteBuffer keys) throws IOException, NoSuchElementException {
        try {
            return Timestamp.fromBytes(rocksDB.get(getArrayByteMinusMinValue(keys)));
        } catch (RocksDBException e) {
            throw new IOException("getWithTimestamp", e);
        }
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) throws IOException {
        try {
            rocksDB.put(getArrayByteMinusMinValue(key), getArray(value));
        } catch (RocksDBException exp) {
            throw new IOException(exp);
        }
    }

    /**
     * method for upsert with Timestamp.
     *
     * @param keys   ByteBuffer keys
     * @param values ByteBuffer values
     * @throws IOException may be throw IOException
     */
    public void upsertWithTimestamp(@NotNull final ByteBuffer keys,
                                    @NotNull final ByteBuffer values) throws IOException {
        try {
            rocksDB.put(getArrayByteMinusMinValue(keys),
                    Timestamp.fromPresent(values, System.currentTimeMillis()).toBytes());
        } catch (RocksDBException e) {
            throw new IOException("upsertWithTimestamp", e);
        }
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) throws IOException {
        try {
            rocksDB.delete(getArrayByteMinusMinValue(key));
        } catch (RocksDBException exp) {
            throw new IOException(exp);
        }
    }

    /**
     * method for remove with Timestamp.
     *
     * @param key ByteBuffer keys
     * @throws IOException may be throw IOException
     */
    public void removeWithTimestamp(@NotNull final ByteBuffer key) throws IOException {
        try {
            rocksDB.put(getArrayByteMinusMinValue(key),
                    Timestamp.timestamp(System.currentTimeMillis()).toBytes());
        } catch (RocksDBException e) {
            throw new IOException("removeWithTimestamp", e);
        }
    }

    private byte[] getArrayByteMinusMinValue(@NotNull final ByteBuffer buffer) {
        synchronized (this) {
            final byte[] body = getArray(buffer);

            for (int i = 0; i < body.length; i++) {
                body[i] -= Byte.MIN_VALUE;
            }

            return body;
        }
    }

    public static byte[] getArray(@NotNull final ByteBuffer buffer) {
        final ByteBuffer duplicate = buffer.duplicate();
        final byte[] body = new byte[duplicate.remaining()];

        duplicate.get(body);

        return body;
    }

    @Override
    public void close() throws IOException {
        try {
            rocksDB.closeE();
        } catch (RocksDBException exp) {
            throw new IOException(exp);
        }
    }

    @Override
    public void compact() throws IOException {
        try {
            rocksDB.compactRange();
        } catch (RocksDBException exp) {
            throw new IOException(exp);
        }
    }

    public static class RocksDBIterator implements Iterator<Record>, AutoCloseable {

        private final RocksIterator iterator;

        RocksDBIterator(@NotNull final RocksIterator iterator) {
            this.iterator = iterator;
        }

        @Override
        public void close() {
            iterator.close();
        }

        @Override
        public boolean hasNext() {
            return iterator.isValid();
        }

        @Override
        public Record next() {
            if (hasNext()) {
                final ByteBuffer key = getByteBufferPlusMinValue(iterator.key());
                final ByteBuffer value = ByteBuffer.wrap(iterator.value());

                final Record result = Record.of(key, value);
                iterator.next();
                return result;
            } else {
                throw new IllegalStateException("Iterator doesn't have the next");
            }
        }

        private ByteBuffer getByteBufferPlusMinValue(@NotNull final byte[] byteArray) {
            final byte[] body = byteArray.clone();

            for (int i = 0; i < body.length; i++) {
                body[i] += Byte.MIN_VALUE;
            }

            return ByteBuffer.wrap(body);
        }
    }
}
package ru.mail.polis.dao;

import org.jetbrains.annotations.NotNull;
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

import static java.lang.Byte.MIN_VALUE;
import static org.rocksdb.BuiltinComparator.BYTEWISE_COMPARATOR;

public class SimpleDAOImpl implements DAO {

    private final RocksDB rocksDB;

    private SimpleDAOImpl(final RocksDB rocksDB) {
        this.rocksDB = rocksDB;
    }

    private byte[] convertValuesSubMinValue(final ByteBuffer byteBuffer) {
        synchronized (this) {
            final byte[] array = getArray(byteBuffer);
            for (int i = 0; i < array.length; i++) {
                array[i] -= MIN_VALUE;
            }
            return array;
        }
    }

    private static ByteBuffer convertValuesAddMinValue(final byte[] array) {
        final byte[] clone = array.clone();
        for (int i = 0; i < array.length; i++) {
            clone[i] += MIN_VALUE;
        }
        return ByteBuffer.wrap(clone);
    }
    
    /**
    *getArray impl getArray from buffer.
    */
    public static byte[] getArray(final ByteBuffer buffer) {
        final ByteBuffer copy = buffer.duplicate();
        final byte[] value = new byte[copy.remaining()];
        copy.get(value);
        return value;
    }

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull final ByteBuffer from) {
        final RocksIterator rocksIterator = rocksDB.newIterator();
        final byte[] array = convertValuesSubMinValue(from);
        rocksIterator.seek(array);

        return new IteratorImpl(rocksIterator);
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) throws IOException {
        try {
            final byte[] keyArray = convertValuesSubMinValue(key);
            final byte[] valueArray = getArray(value);
            rocksDB.put(keyArray, valueArray);
        } catch (RocksDBException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) throws IOException {
        try {
            final byte[] array = convertValuesSubMinValue(key);
            rocksDB.delete(array);
        } catch (RocksDBException e) {
            throw new IOException(e);
        }
    }

    @NotNull
    @Override
    public ByteBuffer get(@NotNull final ByteBuffer key) throws IOException, NoSuchElementException {
        try {
            final byte[] array = convertValuesSubMinValue(key);
            final byte[] value = rocksDB.get(array);
            if (value == null) {
                throw new NoSuchElementLite("No element for given key " + key.toString());
            }
            return ByteBuffer.wrap(value);
        } catch (RocksDBException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void close() {
        rocksDB.close();
    }

    static SimpleDAOImpl init(final File data) throws IOException {
        final Options options = new Options();
        options.setComparator(BYTEWISE_COMPARATOR);
        options.setCreateIfMissing(true);
        try {
            final RocksDB rocksDB = RocksDB.open(options, data.getPath());
            return new SimpleDAOImpl(rocksDB);
        } catch (RocksDBException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void compact() throws IOException {
        try {
            rocksDB.compactRange();
        } catch (RocksDBException e) {
            throw new IOException(e);
        }
    }

    public static class IteratorImpl implements Iterator<Record> {

        private final RocksIterator iterator;

        IteratorImpl(final RocksIterator rocksIterator) {
            iterator = rocksIterator;
        }

        @Override
        public boolean hasNext() {
            return iterator.isValid();
        }

        @Override
        public Record next() {
            if (hasNext()) {
                final ByteBuffer key = SimpleDAOImpl.convertValuesAddMinValue(iterator.key());
                final Record record = Record.of(key, ByteBuffer.wrap(iterator.value()));
                iterator.next();
                return record;
            } else {
                throw new IllegalStateException("EOF");
            }
        }
    }
}

package ru.mail.polis.dao.impl;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import org.rocksdb.*;
import org.rocksdb.util.BytewiseComparator;

import ru.mail.polis.Record;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.dao.Iters;

public class RocksDAO implements DAO {
    private RocksDB db;

    public RocksDAO(@NotNull final File data) throws RockDBNewExceptionLite {
        RocksDB.loadLibrary();
        final Options options = new Options().setCreateIfMissing(true);
        options.setComparator(new BytewiseComparator(new ComparatorOptions()));
        try {
            db = RocksDB.open(options, data.getAbsolutePath());
        } catch (RocksDBException e) {
            throw new RockDBNewExceptionLite("error with open db", e);
        }
    }

    private ByteBuffer deepCopy(final ByteBuffer src) {
        ByteBuffer clone = ByteBuffer.allocate(src.capacity());
        src.rewind();
        clone.put(src);
        src.rewind();
        clone.flip();
        return clone;
    }

    public void upsert(
            @NotNull final ByteBuffer key,
            @NotNull final ByteBuffer value) throws RockDBNewExceptionLite {
        try {
            db.put(deepCopy(key).array(), deepCopy(value).array());
        } catch (RocksDBException e) {
            throw new RockDBNewExceptionLite("error with upsert", e);
        }
    }

    public void remove(@NotNull ByteBuffer key) throws RockDBNewExceptionLite {
        try {
            db.delete(deepCopy(key).array());
        } catch (RocksDBException e) {
            throw new RockDBNewExceptionLite("error with remove", e);
        }
    }

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull ByteBuffer from) {
        return new RocksRecordIterator(db, from);
    }

    @Override
    public void close() throws IOException {
        db.close();
    }

    @NotNull
    @Override
    public Iterator<Record> range(@NotNull ByteBuffer from, @Nullable ByteBuffer to) {
        assert to != null;
        return Iters.until(new RocksRecordIterator(db, from),
                Record.of(to, ByteBuffer.allocate(0)));
    }

    @NotNull
    @Override
    public ByteBuffer get(@NotNull ByteBuffer key) throws NoSuchElementException, RockDBNewExceptionLite {
        byte[] bytes = null;
        try {
            bytes = db.get(deepCopy(key).array());
        } catch (RocksDBException e) {
            throw new RockDBNewExceptionLite("error with bitebuff get", e);
        }
        if (bytes == null) {
            throw new NoSuchElementLite();
        }
        return ByteBuffer.wrap(bytes);
    }

    @Override
    public void compact() {

    }
}

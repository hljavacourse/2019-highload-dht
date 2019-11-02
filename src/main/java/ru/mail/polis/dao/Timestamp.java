package ru.mail.polis.dao;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.List;

public class Timestamp {

    private final State state;
    private final long timestamp;
    private final ByteBuffer present;

    enum State {
        PRESENT((byte) 1),
        REMOVED((byte) -1),
        ABSENT((byte) 0);

        final byte value;

        State(final byte value) {
            this.value = value;
        }

        static State fromValue(final byte value) {
            if (value == REMOVED.value) {
                return REMOVED;
            }
            if (value == PRESENT.value) {
                return PRESENT;
            }
            return ABSENT;
        }
    }

    private Timestamp(final long timestamp,
                      final ByteBuffer present,
                      final State type) {
        this.timestamp = timestamp;
        this.state = type;
        this.present = present;
    }

    static Timestamp fromPresent(final ByteBuffer present, final long timestamp) {
        return new Timestamp(timestamp, present, State.PRESENT);
    }

    static Timestamp timestamp(final long timestamp) {
        return new Timestamp(timestamp, null, State.REMOVED);
    }

    private long getTimestamp() {
        return timestamp;
    }

    public boolean isPresent() {
        return state == State.PRESENT;
    }

    public boolean isAbsent() {
        return state == State.ABSENT;
    }

    public boolean isRemoved() {
        return state == State.REMOVED;
    }

    public byte[] getPresentAsBytes() throws IOException {
        final ByteBuffer duplicate = getPresent().duplicate();
        final byte[] result = new byte[duplicate.remaining()];
        duplicate.get(result);
        return result;
    }

    private ByteBuffer getPresent() throws IOException {
        if (!isPresent()) {
            throw new IOException("value is not present");
        }
        return present;
    }

    public static Timestamp merge(final List<Timestamp> responses) {
        if (responses.size() == 1) {
            return responses.get(0);
        } else {
            return responses.stream()
                    .filter(timestamp -> !timestamp.isAbsent())
                    .max(Comparator.comparingLong(Timestamp::getTimestamp))
                    .orElseGet(Timestamp::getAbsent);
        }
    }

    public static Timestamp getAbsent() {
        return new Timestamp(-1, null, State.ABSENT);
    }

    public static Timestamp fromBytes(final byte[] bytes) {
        if (bytes == null) {
            return getAbsent();
        }
        final ByteBuffer buffer = ByteBuffer.wrap(bytes);
        final State recordType = State.fromValue(buffer.get());
        return new Timestamp(buffer.getLong(), buffer, recordType);
    }

    public byte[] toBytes() {
        int length = 0;
        if (isPresent()) {
            length = present.remaining();
        }
        final ByteBuffer byteBuffer = ByteBuffer.allocate(1 + Long.BYTES + length);
        byteBuffer.put(state.value);
        byteBuffer.putLong(getTimestamp());
        if (isPresent()) {
            byteBuffer.put(present.duplicate());
        }
        return byteBuffer.array();
    }
}

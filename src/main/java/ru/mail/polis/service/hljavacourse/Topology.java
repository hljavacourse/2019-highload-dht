package ru.mail.polis.service.hljavacourse;

import org.jetbrains.annotations.NotNull;

import javax.annotation.concurrent.ThreadSafe;
import java.nio.ByteBuffer;
import java.util.Set;

@ThreadSafe
public interface Topology<T> {

    @NotNull
    T primaryFor(@NotNull final ByteBuffer key);

    boolean isMe(@NotNull final T node);

    @NotNull
    Set<T> all();

    String[] replicas(ByteBuffer id, int count);

    String getMe();
}

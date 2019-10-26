package ru.mail.polis.dao;

import java.util.NoSuchElementException;

public class NoSuchElementLite extends NoSuchElementException {

    private static final long serialVersionUID = 69L;

    NoSuchElementLite(final String message) {
        super(message);
    }

    @Override
    @SuppressWarnings("UnsynchronizedOverridesSynchronized")
    public Throwable fillInStackTrace() {
        synchronized (this) {
            return this;
        }
    }
}

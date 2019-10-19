package ru.mail.polis.dao.impl;

import java.io.IOException;

public class RockDBNewExceptionLite extends IOException {
    public RockDBNewExceptionLite (final String message, final Throwable cause) {
        super(message, cause);
    }
}
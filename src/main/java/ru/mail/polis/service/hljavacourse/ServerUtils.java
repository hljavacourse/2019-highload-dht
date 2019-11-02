package ru.mail.polis.service.hljavacourse;

import one.nio.http.HttpServerConfig;
import one.nio.http.Response;
import one.nio.server.AcceptorConfig;

import java.io.IOException;

class ServerUtils {

    private ServerUtils() {
    }

    /**
     * Method to get for HttpServerConfig.
     *
     * @param port int port
     * @return HttpServerConfig
     */
    static HttpServerConfig from(final int port) {
        final AcceptorConfig ac = new AcceptorConfig();
        ac.port = port;
        ac.reusePort = true;
        ac.deferAccept = true;

        final HttpServerConfig config = new HttpServerConfig();
        config.acceptors = new AcceptorConfig[]{ac};
        return config;
    }

    @FunctionalInterface
    interface Action {
        Response act() throws IOException;
    }
}

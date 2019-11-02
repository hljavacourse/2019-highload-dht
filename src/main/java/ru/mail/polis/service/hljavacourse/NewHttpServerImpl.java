package ru.mail.polis.service.hljavacourse;

import one.nio.http.HttpClient;
import one.nio.http.HttpServer;
import one.nio.http.HttpServerConfig;
import one.nio.http.HttpSession;
import one.nio.http.Path;
import one.nio.http.Request;
import one.nio.http.Response;
import one.nio.net.ConnectionString;
import one.nio.net.Socket;
import one.nio.server.AcceptorConfig;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.Record;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.dao.SimpleDAOImpl;
import ru.mail.polis.service.Service;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.Executor;

public final class NewHttpServerImpl extends HttpServer implements Service {

    private final SimpleDAOImpl dao;

    private final Executor executor;

    private final Logger log = LogManager.getLogger("default");

    private final Topology<String> topology;

    private final Map<String, HttpClient> pool;

    private final int size;

    private static final String PROXY_HEADER = "X-OK-Proxy: True";

    private final RF rf;

    private NewHttpServerImpl(
            final HttpServerConfig config,
            final DAO dao,
            final Executor executor,
            final Topology<String> topology
    ) throws IOException {
        super(config);
        this.dao = (SimpleDAOImpl) dao;
        this.topology = topology;
        this.executor = executor;
        this.rf = new RF(topology.all().size() / 2 + 1, topology.all().size());
        this.size = topology.all().size();

        pool = new HashMap<>();
        for (final String node : topology.all()) {
            if (topology.isMe(node)) {
                continue;
            }

            assert !pool.containsKey(node);
            pool.put(node, new HttpClient(new ConnectionString(node + "?timeout=100")));
        }
    }

    /**
     * Static method for create NewHttpServerImpl.
     *
     * @param port     - int port
     * @param dao      - DAO dao
     * @param executor - Executor executor
     * @param nodes    - Topology nodes
     * @return new NewHttpServerImpl
     * @throws IOException throws IOException
     */
    public static Service create(final int port,
                                 final DAO dao,
                                 final Executor executor,
                                 final Topology<String> nodes) throws IOException {
        final AcceptorConfig ac = new AcceptorConfig();
        ac.port = port;

        final HttpServerConfig config = new HttpServerConfig();
        config.acceptors = new AcceptorConfig[]{ac};
        config.maxWorkers = Runtime.getRuntime().availableProcessors();
        config.queueTime = 10;

        return new NewHttpServerImpl(config, dao, executor, nodes);
    }

    @Override
    public HttpSession createSession(final Socket socket) {
        return new StorageSession(socket, this);
    }

    @Path("/v0/status")
    public Response status() {
        return Response.ok("I'm OK");
    }

    private void entity(@NotNull final Request request, @NotNull final HttpSession session) throws IOException {
        final String id = request.getParameter("id=");
        if (isNotNullOrEmpty(id)) {
            session.sendError(Response.BAD_REQUEST, "No id");
            return;
        }

        boolean proxied = false;
        if (request.getHeader(PROXY_HEADER) != null) {
            proxied = true;
        }

        final String replicas = request.getParameter("replicas");
        final RF newRf = RF.getRf(replicas, session, rf, size);
        final ByteBuffer key = ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));

        if (proxied || topology.all().size() > 1) {
            final Coordinators coordinators = new Coordinators(topology, pool, dao, proxied);
            final String[] replica;
            if (proxied) {
                replica = new String[]{topology.getMe()};
            } else {
                replica = topology.replicas(key, newRf.getFrom());
            }
            coordinators.request(replica, request, newRf.getAck(), session);
        } else {
            final int method = request.getMethod();
            switch (method) {
                case Request.METHOD_GET:
                    executeAsync(session, () -> get(id));
                    break;

                case Request.METHOD_PUT:
                    executeAsync(session, () -> upsert(id, request.getBody()));
                    break;

                case Request.METHOD_DELETE:
                    executeAsync(session, () -> delete(id));
                    break;

                default:
                    session.sendError(Response.METHOD_NOT_ALLOWED, "Wrong method");
                    break;
            }
        }
    }

    private Response get(@NotNull final String id) throws IOException {
        try {
            final ByteBuffer wrap = ByteBuffer.wrap(id.getBytes(Charset.defaultCharset()));
            final ByteBuffer value = dao.get(wrap);
            final byte[] bytes = SimpleDAOImpl.getArray(value);
            return new Response(Response.OK, bytes);
        } catch (NoSuchElementException e) {
            return new Response(Response.NOT_FOUND, Response.EMPTY);
        }
    }

    private Response delete(@NotNull final String id) throws IOException {
        dao.remove(ByteBuffer.wrap(id.getBytes(Charset.defaultCharset())));
        return new Response(Response.ACCEPTED, Response.EMPTY);
    }

    private Response upsert(@NotNull final String id, @NotNull final byte[] value) throws IOException {
        dao.upsert(ByteBuffer.wrap(id.getBytes(Charset.defaultCharset())), ByteBuffer.wrap(value));
        return new Response(Response.CREATED, Response.EMPTY);
    }

    private void entities(@NotNull final Request request, @NotNull final HttpSession session) throws IOException {
        final String startKey = "start=";
        final String startId = request.getParameter(startKey);

        if (isNotNullOrEmpty(startId)) {
            session.sendError(Response.BAD_REQUEST, "No start");
            return;
        }

        final String wrong_method = "Wrong method";
        if (request.getMethod() != Request.METHOD_GET) {
            session.sendError(Response.METHOD_NOT_ALLOWED, wrong_method);
            return;
        }

        final String endKey = "end=";
        String end = request.getParameter(endKey);
        if (end != null && end.isEmpty()) end = null;

        try {
            final ByteBuffer wrap = ByteBuffer.wrap(startId.getBytes(Charset.defaultCharset()));
            final Iterator<Record> records = dao.range(wrap,
                    end == null ? null : ByteBuffer.wrap(end.getBytes(Charset.defaultCharset())));
            ((StorageSession) session).stream(records);
        } catch (IOException e) {
            session.sendError(Response.INTERNAL_ERROR, e.getMessage());
        }
    }

    private boolean isNotNullOrEmpty(final String startId) {
        return startId == null || startId.isEmpty();
    }

    @Override
    public void handleDefault(@NotNull final Request request, @NotNull final HttpSession session) throws
            IOException {
        final String path = request.getPath();
        switch (path) {
            case "/v0/entity":
                entity(request, session);
                break;
            case "/v0/entities":
                entities(request, session);
                break;
            default:
                session.sendError(Response.BAD_REQUEST, "Wrong path");
                break;
        }
    }

    private void executeAsync(@NotNull final HttpSession session, @NotNull final ServerUtils.Action action) {
        executor.execute(() -> {
            execute(session, action);
        });
    }

    private void execute(@NotNull final HttpSession session, @NotNull final ServerUtils.Action action) {
        try {
            session.sendResponse(action.act());
        } catch (IOException e) {
            try {
                session.sendError(Response.INTERNAL_ERROR, e.getMessage());
            } catch (IOException ex) {
                log.error(ex.getMessage());
            }
        }
    }
}

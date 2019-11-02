package ru.mail.polis.service.hljavacourse;

import one.nio.http.HttpClient;
import one.nio.http.HttpException;
import one.nio.http.HttpSession;
import one.nio.http.Request;
import one.nio.http.Response;
import one.nio.pool.PoolException;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.dao.SimpleDAOImpl;
import ru.mail.polis.dao.Timestamp;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

class Coordinators {

    private final SimpleDAOImpl dao;

    private final Topology<String> topology;

    private final Map<String, HttpClient> pool;

    private final boolean proxied;

    private static final Logger logger = Logger.getLogger(Coordinators.class.getName());

    private static final String PROXY_HEADER = "X-OK-Proxy: True";
    private static final String ENTITY_HEADER = "/v0/entity?id=";

    Coordinators(final Topology<String> topology,
                 final Map<String, HttpClient> pool,
                 final DAO dao,
                 final boolean proxied) {
        this.dao = (SimpleDAOImpl) dao;
        this.topology = topology;
        this.pool = pool;
        this.proxied = proxied;
    }

    private Response delete(final String[] nodes,
                            final Request request,
                            final int acks,
                            final boolean proxied) {
        final String id = request.getParameter("id=");
        final ByteBuffer key = ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));
        int asks = 0;
        for (final String node : nodes) {
            try {
                if (topology.isMe(node)) {
                    dao.removeWithTimestamp(key);
                    asks++;
                } else {
                    request.addHeader(PROXY_HEADER);
                    final Response resp = pool.get(node).delete(ENTITY_HEADER + id, PROXY_HEADER);
                    if (resp.getStatus() == 202) {
                        asks++;
                    }
                }
            } catch (IOException | HttpException | InterruptedException | PoolException e) {
                logger.log(Level.SEVERE, "delete in Coordinators", e);
            }
        }
        if (asks >= acks || proxied) {
            return new Response(Response.ACCEPTED, Response.EMPTY);
        } else {
            return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
        }
    }

    private Response upsert(final String[] nodes,
                            final Request request,
                            final int acks,
                            final boolean proxied) {
        final String id = request.getParameter("id=");
        final ByteBuffer key = ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));
        int asks = 0;
        for (final String node : nodes) {
            try {
                if (topology.isMe(node)) {
                    dao.upsertWithTimestamp(key, ByteBuffer.wrap(request.getBody()));
                    asks++;
                } else {
                    request.addHeader(PROXY_HEADER);
                    final Response response = pool.get(node).put(ENTITY_HEADER + id, request.getBody(), PROXY_HEADER);
                    if (response.getStatus() == 201) {
                        asks++;
                    }
                }
            } catch (IOException | HttpException | PoolException | InterruptedException e) {
                logger.log(Level.SEVERE, "upsert in Coordinators", e);
            }
        }
        if (asks >= acks || proxied) {
            return new Response(Response.CREATED, Response.EMPTY);
        } else {
            return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
        }
    }

    private Response get(final String[] nodes,
                         final Request request,
                         final int acks,
                         final boolean proxied) throws IOException {
        final String id = request.getParameter("id=");
        final ByteBuffer key = ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));
        int asks = 0;
        final List<Timestamp> responses = new ArrayList<>();
        for (final String node : nodes) {
            try {
                if (performResponse(request, id, key, responses, node)) continue;
                asks++;
            } catch (HttpException | PoolException | InterruptedException e) {
                logger.log(Level.SEVERE, "get in Coordinators", e);
            }
        }
        if (asks >= acks || proxied) {
            return response(nodes, responses);
        } else {
            return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
        }
    }

    private boolean performResponse(final Request request,
                                    final String id, final ByteBuffer key,
                                    final List<Timestamp> responses,
                                    final String node
    ) throws IOException, InterruptedException, PoolException, HttpException {
        Response response;
        if (topology.isMe(node)) {
            final Timestamp val = dao.getWithTimestamp(key);
            if (val.isAbsent()) {
                response = new Response(Response.NOT_FOUND, Response.EMPTY);
            } else {
                response = new Response(Response.OK, val.toBytes());
            }
        } else {
            request.addHeader(PROXY_HEADER);
            response = pool.get(node).get(ENTITY_HEADER + id, PROXY_HEADER);
        }
        if (response.getStatus() == 404 && response.getBody().length == 0) {
            responses.add(Timestamp.getAbsent());
        } else if (response.getStatus() == 500) {
            return true;
        } else {
            responses.add(Timestamp.fromBytes(response.getBody()));
        }
        return false;
    }

    private Response response(final String[] replicaNodes, final List<Timestamp> responses) throws IOException {
        final Timestamp mergeResponse = Timestamp.merge(responses);
        if (mergeResponse.isPresent()) {
            if (!proxied && replicaNodes.length == 1) {
                return new Response(Response.OK, mergeResponse.getPresentAsBytes());
            } else if (proxied && replicaNodes.length == 1) {
                return new Response(Response.OK, mergeResponse.toBytes());
            } else {
                return new Response(Response.OK, mergeResponse.getPresentAsBytes());
            }
        } else if (mergeResponse.isRemoved()) {
            return new Response(Response.NOT_FOUND, mergeResponse.toBytes());
        } else {
            return new Response(Response.NOT_FOUND, Response.EMPTY);
        }
    }

    void request(final String[] replicaClusters,
                 final Request request,
                 final int acks,
                 final HttpSession session) throws IOException {
        try {
            switch (request.getMethod()) {
                case Request.METHOD_GET:
                    session.sendResponse(get(replicaClusters, request, acks, proxied));
                    break;
                case Request.METHOD_PUT:
                    session.sendResponse(upsert(replicaClusters, request, acks, proxied));
                    break;
                case Request.METHOD_DELETE:
                    session.sendResponse(delete(replicaClusters, request, acks, proxied));
                    break;
                default:
                    session.sendError(Response.METHOD_NOT_ALLOWED, "Wrong method");
                    break;
            }
        } catch (IOException e) {
            session.sendError(Response.GATEWAY_TIMEOUT, e.getMessage());
        }
    }
}

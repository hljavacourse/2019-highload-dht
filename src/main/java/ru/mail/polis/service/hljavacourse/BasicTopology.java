package ru.mail.polis.service.hljavacourse;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class BasicTopology implements Topology<String> {

    private final List<String> nodes;

    private final String me;

    /**
     * BasicTopology constructor.
     *
     * @param nodes  Set (of String) nodes
     * @param me    String
     */
    public BasicTopology(final Set<String> nodes, final String me) {
        assert nodes.contains(me);
        this.nodes = new ArrayList<>(nodes);
        this.me = me;
    }

    @Override
    public String primaryFor(final ByteBuffer key) {
        final int hash = key.hashCode();
        final int node = (hash & Integer.MAX_VALUE) % nodes.size();
        return nodes.get(node);
    }

    @Override
    public Set<String> all() {
        return new HashSet<>(nodes);
    }

    @Override
    public boolean isMe(final String node) {
        return me.equals(node);
    }

    @Override
    public String getMe() {
        return me;
    }

    @Override
    public String[] replicas(final ByteBuffer id, final int count) {
        final String[] result = new String[count];
        int ind = (id.hashCode() & Integer.MAX_VALUE) % nodes.size();
        for (int j = 0; j < count; j++) {
            result[j] = nodes.get(ind);
            ind = (ind + 1) % nodes.size();
        }

        return result;
    }
}
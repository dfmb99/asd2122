package protocols;

import pt.unl.fct.di.novasys.network.data.Host;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class Channel {

    public final int id;
    public final Properties channelProps;

    public final Map<Host, Integer> connectionInUseBy;
    public final Set<Host> openConnections;
    public final Set<Host> pendingConnections;

    public Channel(int id, Properties channelProps) {
        this.id = id;
        this.channelProps = channelProps;

        connectionInUseBy = new ConcurrentHashMap<>();
        openConnections = ConcurrentHashMap.newKeySet();
        pendingConnections = ConcurrentHashMap.newKeySet();
    }

    public void incrementConnectionUses(Host peer) {
        connectionInUseBy.merge(peer, 1, (prev, val) -> prev+val);
    }

    public int decrementConnectionUses(Host peer) {
        connectionInUseBy.merge(peer, 0, (prev, val) -> Math.max(0, prev-1));
        return connectionInUseBy.get(peer);
    }
}

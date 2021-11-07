package protocols;

import pt.unl.fct.di.novasys.network.data.Host;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class Channel {

    public final int id;
    public final Properties channelProps;

    public final Set<Host> openConnections;
    public final Set<Host> pendingConnections;

    public Channel(int id, Properties channelProps) {
        this.id = id;
        this.channelProps = channelProps;

        openConnections = ConcurrentHashMap.newKeySet();
        pendingConnections = ConcurrentHashMap.newKeySet();
    }
}

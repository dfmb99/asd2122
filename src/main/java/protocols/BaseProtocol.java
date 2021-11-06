package protocols;

import org.apache.logging.log4j.Logger;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.channel.tcp.TCPChannel;
import pt.unl.fct.di.novasys.channel.tcp.events.*;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public abstract class BaseProtocol extends GenericProtocol {

    private final Logger logger;

    protected int channelId;
    protected final Host self;

    private final Set<Host> openConnections;
    private final Set<Host> pendingConnections;
    private final Map<Host, List<ProtoMessage>> pendingMessages;

    public BaseProtocol(Properties props, Host self, String protocolName, short protocolId, Logger logger, boolean createChannel) throws IOException {
        super(protocolName, protocolId);
        this.logger = logger;

        this.self = self;

        if(createChannel) {
            Properties channelProps = new Properties();
            channelProps.setProperty(TCPChannel.ADDRESS_KEY, props.getProperty("address")); //The address to bind to
            channelProps.setProperty(TCPChannel.PORT_KEY, props.getProperty("port")); //The port to bind to
            channelProps.setProperty(TCPChannel.METRICS_INTERVAL_KEY, props.getProperty("channel_metrics_interval", "1000")); //The interval to receive channel metrics
            channelProps.setProperty(TCPChannel.HEARTBEAT_INTERVAL_KEY, props.getProperty("heartbeat_interval", "1000")); //Heartbeats interval for established connections
            channelProps.setProperty(TCPChannel.HEARTBEAT_TOLERANCE_KEY, props.getProperty("heartbeat_tolerance", "3000")); //Time passed without heartbeats until closing a connection
            channelProps.setProperty(TCPChannel.CONNECT_TIMEOUT_KEY, props.getProperty("tcp_timeout", "6000")); //TCP connect timeout
            channelId = createChannel(TCPChannel.NAME, channelProps);
        }

        openConnections = ConcurrentHashMap.newKeySet();
        pendingConnections = ConcurrentHashMap.newKeySet();
        pendingMessages = new ConcurrentHashMap<>();
    }

    @Override
    public void init(Properties properties) {}

    public void registerChannelEvents() throws HandlerRegistrationException {
        registerChannelEventHandler(channelId, OutConnectionDown.EVENT_ID, this::uponOutConnectionDown);
        registerChannelEventHandler(channelId, OutConnectionFailed.EVENT_ID, this::uponOutConnectionFailed);
        registerChannelEventHandler(channelId, OutConnectionUp.EVENT_ID, this::uponOutConnectionUp);
        registerChannelEventHandler(channelId, InConnectionUp.EVENT_ID, this::uponInConnectionUp);
        registerChannelEventHandler(channelId, InConnectionDown.EVENT_ID, this::uponInConnectionDown);
    }

    protected void mendConnection(Host peer) {
        if(!openConnections.contains(peer) && !pendingConnections.contains(peer)) {
            openConnection(peer);
            pendingConnections.add(peer);
        }
    }

    protected void breakConnection(Host peer) {
        openConnections.remove(peer);
        pendingConnections.remove(peer);
        pendingMessages.remove(peer);
        closeConnection(peer);
    }

    protected void dispatchMessage(ProtoMessage message, Host peer) {
        if(openConnections.contains(peer)) {
            sendMessage(message, peer);
        }
        else if(pendingConnections.contains(peer)) {
            pendingMessages.get(peer).add(message);
        }
        else {
            openConnection(peer);
            pendingConnections.add(peer);
            pendingMessages.put(peer, new LinkedList<>(Collections.singleton(message)));
        }
    }

    protected void uponOutConnectionUp(OutConnectionUp event, int channelId) {
        Host peer = event.getNode();
        logger.info("Connection to {} is up", peer);
        openConnections.add(peer);
        pendingConnections.remove(peer);

        for (ProtoMessage m : pendingMessages.get(peer))
            sendMessage(m, peer);

        pendingMessages.remove(peer);
    }

    protected void uponOutConnectionDown(OutConnectionDown event, int channelId) {
        Host peer = event.getNode();
        logger.info("Connection to {} is down cause {}", peer, event.getCause());
        openConnections.remove(peer);
        pendingConnections.remove(peer);
        pendingMessages.remove(peer);
    }


    protected void uponOutConnectionFailed(OutConnectionFailed<ProtoMessage> event, int channelId) {
        Host peer = event.getNode();
        logger.info("Connection to {} failed cause: {}", event.getNode(), event.getCause());
        openConnections.remove(peer);
        pendingConnections.remove(peer);
        pendingMessages.remove(peer);
    }


    protected void uponInConnectionUp(InConnectionUp event, int channelId) {
        Host peer = event.getNode();
        logger.trace("Connection from {} is up", peer);
        openConnections.add(peer);
        pendingConnections.remove(peer);

        for (ProtoMessage m : pendingMessages.get(peer))
            sendMessage(m, peer);

        pendingMessages.remove(peer);
    }


    protected void uponInConnectionDown(InConnectionDown event, int channelId) {
        Host peer = event.getNode();
        logger.trace("Connection from {} is down, cause: {}", peer, event.getCause());
        openConnections.remove(peer);
        pendingMessages.remove(peer);
    }
}

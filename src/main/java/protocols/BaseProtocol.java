package protocols;

import notifications.ChannelCreated;
import org.apache.logging.log4j.Logger;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.exceptions.NoSuchProtocolException;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.babel.generic.ProtoReply;
import pt.unl.fct.di.novasys.babel.internal.IPCEvent;
import pt.unl.fct.di.novasys.channel.tcp.TCPChannel;
import pt.unl.fct.di.novasys.channel.tcp.events.*;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public abstract class BaseProtocol extends GenericProtocol {

    private final Logger logger;

    protected final Host self;
    protected Channel channel;

    protected final Map<Host, Queue<ProtoMessage>> pendingMessages;
    protected final Map<Host, Queue<ProtoMessage>> pendingRetryMessages;

    private final Set<UUID> replies;

    public BaseProtocol(Host self, String protocolName, short protocolId, Logger logger) {
        super(protocolName, protocolId);

        this.logger = logger;
        this.self = self;

        pendingMessages = new HashMap<>();
        pendingRetryMessages = new HashMap<>();
        replies = new HashSet<>();
    }

    @Override
    public void init(Properties properties) {}

    public void createChannel(Properties props) throws IOException {
        Properties channelProps = new Properties();
        channelProps.setProperty(TCPChannel.ADDRESS_KEY, props.getProperty("address")); //The address to bind to
        channelProps.setProperty(TCPChannel.PORT_KEY, props.getProperty("port")); //The port to bind to
        channelProps.setProperty(TCPChannel.METRICS_INTERVAL_KEY, props.getProperty("channel_metrics_interval", "1000")); //The interval to receive channel metrics
        channelProps.setProperty(TCPChannel.HEARTBEAT_INTERVAL_KEY, props.getProperty("heartbeat_interval", "1000")); //Heartbeats interval for established connections
        channelProps.setProperty(TCPChannel.HEARTBEAT_TOLERANCE_KEY, props.getProperty("heartbeat_tolerance", "3000")); //Time passed without heartbeats until closing a connection
        channelProps.setProperty(TCPChannel.CONNECT_TIMEOUT_KEY, props.getProperty("tcp_timeout", "6000")); //TCP connect timeout
        int channelId = createChannel(TCPChannel.NAME, channelProps);
        channel = new Channel(channelId, props);
        triggerNotification(new ChannelCreated(this.channel));
    }

    public void setChannel(Channel channel) {
        this.channel = channel;
        registerSharedChannel(channel.id);
    }

    protected void mendConnection(Host host) {
        if(!channel.openConnections.contains(host) && !channel.pendingConnections.contains(host)) {
            openConnection(host);
            channel.pendingConnections.add(host);
            pendingMessages.put(host, new LinkedList<>());
            pendingRetryMessages.put(host, new LinkedList<>());
        }
    }

    protected void breakConnection(Host host) {
        channel.openConnections.remove(host);
        channel.pendingConnections.remove(host);
        pendingMessages.remove(host);
        pendingRetryMessages.remove(host);
        closeConnection(host);
    }

    protected void dispatchMessageButNotToSelf(ProtoMessage message, Host host) {
        if(!host.equals(self))
            dispatchMessage(message,host);
    }

    protected void dispatchMessage(ProtoMessage message, Host host) {

        if(channel.openConnections.contains(host)) {
            logger.debug("Sent message {} from {} to {} ", message, self, host);
            sendMessage(message, host);
        }
        else if(channel.pendingConnections.contains(host)) {
            logger.debug("Queued message {} from {} to {} ", message, self, host);
            Queue<ProtoMessage> l =  pendingMessages.get(host);
            if(l != null)
                l.add(message);
            else
                pendingMessages.put(host, new LinkedList<>(Collections.singleton(message)));
        }
        else {
            openConnection(host);
            channel.pendingConnections.add(host);
            logger.debug("Queued message {} from {} to {} ", message, self, host);
            pendingMessages.put(host, new LinkedList<>(Collections.singleton(message)));
        }
    }

    protected void dispatchRetryMessageButNotToSelf(ProtoMessage message, Host host) {
        if(!host.equals(self))
            dispatchRetryMessage(message,host);
    }

    protected void dispatchRetryMessage(ProtoMessage message, Host host) {

        if(channel.openConnections.contains(host)) {
            logger.debug("Sent message {} from {} to {} ", message, self, host);
            sendMessage(message, host);
        }
        else if(channel.pendingConnections.contains(host)) {
            logger.debug("Queued message {} from {} to {} ", message, self, host);
            Queue<ProtoMessage> l =  pendingRetryMessages.get(host);
            if(l != null)
                l.add(message);
            else
                pendingRetryMessages.put(host, new LinkedList<>(Collections.singleton(message)));
        }
        else {
            openConnection(host);
            channel.pendingConnections.add(host);
            logger.debug("Queued message {} from {} to {} ", message, self, host);
            pendingRetryMessages.put(host, new LinkedList<>(Collections.singleton(message)));
        }
    }

    protected void sendReplyOnce(ProtoReply reply, short destination, UUID id) {
        if(!replies.contains(id)) {
            replies.add(id);
            sendReply(reply, destination);
        }
    }

    protected void uponOutConnectionUp(OutConnectionUp event, int channelId) {
        Host host = event.getNode();
        logger.debug("Out Connection from {} to {} is up", self, host);
        channel.openConnections.add(host);
        channel.pendingConnections.remove(host);

        Queue<ProtoMessage> msgs = pendingMessages.get(host);
        while (msgs!=null && !msgs.isEmpty()) {
            ProtoMessage m = msgs.poll();
            logger.debug("Sent message {} from {} to {} ", m, self, host);
            sendMessage(m, host);
        }

        Queue<ProtoMessage> rmsgs = pendingRetryMessages.get(host);
        while (rmsgs!=null && !rmsgs.isEmpty()) {
            ProtoMessage m = rmsgs.poll();
            logger.debug("Sent message {} from {} to {} ", m, self, host);
            sendMessage(m, host);
        }
    }

    protected void uponMessageFail(ProtoMessage msg, Host host, short destProto, Throwable throwable, int channelId) {
        logger.error("Message {} to {} failed, reason: {}", msg, host, throwable);
        channel.openConnections.remove(host);
        closeConnection(host);
        openConnection(host);
        channel.pendingConnections.add(host);
    }

    protected void uponMessageFailRetry(ProtoMessage msg, Host host, short destProto, Throwable throwable, int channelId) {
        logger.error("Message {} to {} failed, reason: {}", msg, host, throwable);
        channel.openConnections.remove(host);
        closeConnection(host);
        openConnection(host);
        channel.pendingConnections.add(host);
        Queue<ProtoMessage> l =  pendingRetryMessages.get(host);
        if(l != null)
            l.add(msg);
        else
            pendingRetryMessages.put(host, new LinkedList<>(Collections.singleton(msg)));
    }

    protected void uponOutConnectionDown(OutConnectionDown event, int channelId) {
        Host host = event.getNode();
        logger.debug("Out Connection from {} to {} is down cause {}", self, host, event.getCause());
        channel.openConnections.remove(host);
        channel.pendingConnections.remove(host);
        pendingMessages.remove(host);
    }


    protected void uponOutConnectionFailed(OutConnectionFailed<ProtoMessage> event, int channelId) {
        Host host = event.getNode();
        logger.debug("Out Connection from {} to {} failed cause: {}", self, host, event.getCause());
        channel.openConnections.remove(host);
        channel.pendingConnections.remove(host);
        pendingMessages.remove(host);
    }


    protected void uponInConnectionUp(InConnectionUp event, int channelId) {
        Host host = event.getNode();
        logger.debug("In Connection from {} to {}  is up", host, self);
    }


    protected void uponInConnectionDown(InConnectionDown event, int channelId) {
        Host host = event.getNode();
        logger.debug("In Connection from {} to {} is down, cause: {}", host, self, event.getCause());
    }
}

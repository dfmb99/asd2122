package protocols.dht.chord;

import notifications.ChannelCreated;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.BaseProtocol;
import protocols.dht.chord.messages.overlay.*;
import protocols.dht.chord.messages.search.FindSuccessorMessage;
import protocols.dht.chord.messages.search.FindSuccessorReplyMessage;
import protocols.dht.chord.types.Node;
import protocols.dht.chord.timers.FixFingersTimer;
import protocols.dht.chord.timers.InfoTimer;
import protocols.dht.chord.timers.KeepAliveTimer;
import protocols.dht.chord.timers.StabilizeTimer;
import protocols.dht.replies.LookupReply;
import protocols.dht.requests.LookupRequest;
import protocols.storage.StorageProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.data.Host;
import utils.Ring;

import java.io.IOException;
import java.math.BigInteger;
import java.net.InetAddress;
import java.util.*;

public class ChordProtocol extends BaseProtocol {

    private static final Logger logger = LogManager.getLogger(ChordProtocol.class);

    public static final short PROTOCOL_ID = 20;
    public static final String PROTOCOL_NAME = "ChordProtocol";

    private final int m;
    private final Ring ring;

    private boolean bIsInsideRing;

    private final Node self;
    private Node predecessor;
    private final Node[] fingers;
    private final BigInteger[] fingerPosition;

    public ChordProtocol(Properties props, Host self) throws IOException, HandlerRegistrationException {
        super(props, self, PROTOCOL_NAME, PROTOCOL_ID, logger, true);

        int numberOfNodes = Integer.parseInt(props.getProperty("number_of_nodes"));
        this.m = 2*(int)(Math.log(numberOfNodes) / Math.log(2));
        this.ring = new Ring(BigInteger.TWO.pow(m));

        this.self = new Node(self,m);
        this.predecessor = null;
        this.fingers = new Node[m];
        this.fingerPosition = new BigInteger[m];
        for(int i=0; i<m; i++) {
            fingerPosition[i] = this.self.getId().add(BigInteger.TWO.pow(nextFinger));
        }

        /*--------------------- Register Timer Handlers ----------------------------- */
        registerTimerHandler(KeepAliveTimer.TIMER_ID, this::uponKeepAliveTime);
        registerTimerHandler(StabilizeTimer.TIMER_ID, this::uponStabilizeTime);
        registerTimerHandler(FixFingersTimer.TIMER_ID, this::uponFixFingersTime);
        registerTimerHandler(InfoTimer.TIMER_ID, this::uponInfoTime);

        /*---------------------- Register Channel Events ------------------------------ */
        registerChannelEvents();

        /*--------------------- Register Request Handlers ----------------------------- */
        registerRequestHandler(LookupRequest.REQUEST_TYPE_ID, this::uponLookupRequest);

        /*---------------------- Register Message Handlers -------------------------- */
        registerMessageHandler(channelId, KeepAliveMessage.MSG_ID, this::UponKeepAliveMessage, this::uponKeepAliveMessageFail);

        registerMessageHandler(channelId, JoinRingMessage.MSG_ID, this::uponJoinRingMessage, this::uponMessageFail);
        registerMessageHandler(channelId, JoinRingReplyMessage.MSG_ID, this::uponJoinRingReplyMessage, this::uponMessageFail);

        registerMessageHandler(channelId, GetPredecessorMessage.MSG_ID, this::UponGetPredecessorMessage, this::uponMessageFail);
        registerMessageHandler(channelId, GetPredecessorReplyMessage.MSG_ID, this::UponGetPredecessorReplyMessage, this::uponMessageFail);

        registerMessageHandler(channelId, NotifySuccessorMessage.MSG_ID, this::UponNotifySuccessorMessage, this::uponMessageFail);

        registerMessageHandler(channelId, RestoreFingerMessage.MSG_ID, this::UponRestoreFingerMessage, this::uponMessageFail);
        registerMessageHandler(channelId, RestoreFingerReplyMessage.MSG_ID, this::UponRestoreFingerReplyMessage, this::uponMessageFail);

        registerMessageHandler(channelId, FindSuccessorMessage.MSG_ID, this::UponFindSuccessorMessage, this::uponMessageFail);
        registerMessageHandler(channelId, FindSuccessorReplyMessage.MSG_ID, this::UponFindSuccessorReplyMessage, this::uponMessageFail);

        /*---------------------- Register Message Serializers ---------------------- */
        registerMessageSerializer(channelId, KeepAliveMessage.MSG_ID, KeepAliveMessage.serializer);

        registerMessageSerializer(channelId, JoinRingMessage.MSG_ID, JoinRingMessage.serializer);
        registerMessageSerializer(channelId, JoinRingReplyMessage.MSG_ID, JoinRingReplyMessage.serializer);

        registerMessageSerializer(channelId, GetPredecessorMessage.MSG_ID, GetPredecessorMessage.serializer);
        registerMessageSerializer(channelId, GetPredecessorReplyMessage.MSG_ID, GetPredecessorReplyMessage.serializer);

        registerMessageSerializer(channelId, NotifySuccessorMessage.MSG_ID, NotifySuccessorMessage.serializer);

        registerMessageSerializer(channelId, RestoreFingerMessage.MSG_ID, RestoreFingerMessage.serializer);
        registerMessageSerializer(channelId, RestoreFingerReplyMessage.MSG_ID, RestoreFingerReplyMessage.serializer);

        registerMessageSerializer(channelId, FindSuccessorMessage.MSG_ID, FindSuccessorMessage.serializer);
        registerMessageSerializer(channelId, FindSuccessorReplyMessage.MSG_ID, FindSuccessorReplyMessage.serializer);
    }

    private void setSuccessor(Node successor) {
        this.fingers[0] = successor;
        mendConnection(successor.getHost());
    }

    private Node getSuccessor() {
        return fingers[0];
    }

    private void setPredecessor(Node predecessor) {
        this.predecessor = predecessor;
        mendConnection(predecessor.getHost());
    }

    private void setFinger(int i, Node finger) {
        this.fingers[i] = finger;
        mendConnection(finger.getHost());
    }

    @Override
    public void init(Properties props) {
        enterRing(props);

        int keepAliveInterval = Integer.parseInt(props.getProperty("chord_keep_alive_interval", "4000"));
        setupPeriodicTimer(new KeepAliveTimer(), keepAliveInterval, keepAliveInterval);

        int stabilizeInterval = Integer.parseInt(props.getProperty("chord_stabilize_interval", "1000"));
        setupPeriodicTimer(new StabilizeTimer(), stabilizeInterval, stabilizeInterval);

        int fixFingerInterval = Integer.parseInt(props.getProperty("chord_fix_finger_interval", "1000"));
        setupPeriodicTimer(new FixFingersTimer(), fixFingerInterval, fixFingerInterval);

        int metricsInterval = Integer.parseInt(props.getProperty("protocol_metrics_interval", "1000"));
        if (metricsInterval > 0)
            setupPeriodicTimer(new InfoTimer(), metricsInterval, metricsInterval);

        logger.info("Hello, I am {}", self);
    }

    private void enterRing(Properties props) {
        if(!props.containsKey("contact")) {
            bIsInsideRing = true;
            predecessor = self;
            setSuccessor(self);
            triggerNotification(new ChannelCreated(channelId));
            logger.info("Joined the ring {}", self.getHost());
            return;
        }

        bIsInsideRing = false;
        try {
            String contact = props.getProperty("contact");
            String[] hostElems = contact.split(":");
            Node contactNode = new Node(new Host(InetAddress.getByName(hostElems[0]), Short.parseShort(hostElems[1])), m);
            dispatchMessage(new JoinRingMessage(self), contactNode.getHost());
        } catch (Exception e) {
            logger.error("Invalid contact on configuration: '" + props.getProperty("contact"));
            e.printStackTrace();
            System.exit(-1);
        }
    }

    /*--------------------------------- Join Ring -------------------------------------- */

    private void uponJoinRingMessage(JoinRingMessage msg, Host from, short sourceProto, int channelId) {
        logger.info("Received {} from {}", msg, from);
        Node node = msg.getNode();
        if(ring.InBounds(node.getId(), self.getId(), getSuccessor().getId())){
            dispatchMessage(new JoinRingReplyMessage(getSuccessor()), node.getHost());
        }
        else {
            Node closestPrecedingNode = closestPrecedingNode(node.getId());
            dispatchMessage(msg, closestPrecedingNode.getHost());
        }
    }

    private void uponJoinRingReplyMessage(JoinRingReplyMessage msg, Host from, short sourceProto, int channelId) {
        logger.info("Received {} from {}", msg, from);
        bIsInsideRing = true;
        setSuccessor(msg.getNode());
        triggerNotification(new ChannelCreated(channelId));
        logger.info("Joined the ring {}", self.getHost());
    }


    /*--------------------------------- Stabilize -------------------------------------- */

    private void uponStabilizeTime(StabilizeTimer timer, long timerId) {
        if(!bIsInsideRing) return;

        dispatchMessageButNotToSelf(new GetPredecessorMessage(), getSuccessor().getHost());
    }

    private void UponGetPredecessorMessage(GetPredecessorMessage msg, Host from, short sourceProto, int channelId) {
        logger.info("Received {} from {}", msg, from);
        dispatchMessage(new GetPredecessorReplyMessage(predecessor), from);
    }

    private void UponGetPredecessorReplyMessage(GetPredecessorReplyMessage msg, Host from, short sourceProto, int channelId) {
        logger.info("Received {} from {}", msg, from);
        if(ring.InBounds(msg.getPredecessor().getId(), self.getId(), getSuccessor().getId()))
            setSuccessor(msg.getPredecessor());
        dispatchMessage(new NotifySuccessorMessage(), getSuccessor().getHost());
    }

    private void UponNotifySuccessorMessage(NotifySuccessorMessage msg, Host from, short sourceProto, int channelId) {
        logger.info("Received {} from {}", msg, from);
        Node node = new Node(from, m);
        if(ring.InBounds(node.getId(), predecessor.getId(), self.getId()))
            setPredecessor(node);
    }

    /*----------------------------------- Fix Fingers --------------------------------- */

    private int nextFinger = 1;
    private void uponFixFingersTime(FixFingersTimer timer, long timerId) {
        if(!bIsInsideRing) return;

        if(nextFinger >= m) nextFinger = 1;
        Node closestPrecedingNode = closestPrecedingNode(fingerPosition[nextFinger]);
        if(closestPrecedingNode.equals(self))
            setFinger(nextFinger, getSuccessor());
        else
            dispatchMessage(new RestoreFingerMessage(nextFinger, fingerPosition[nextFinger], self.getHost()), closestPrecedingNode.getHost());
        nextFinger++;
    }

    private void UponRestoreFingerMessage(RestoreFingerMessage msg, Host from, short sourceProto, int channelId) {
        logger.info("Received {} from {}", msg, from);
        if(ring.InBounds(msg.getKey(), self.getId(), getSuccessor().getId()))
            dispatchMessage(new RestoreFingerReplyMessage(msg.getFinger(), getSuccessor()), msg.getHost());
        else {
            Node closestPrecedingNode = closestPrecedingNode(msg.getKey());
            dispatchMessage(msg, closestPrecedingNode.getHost());
        }
    }

    private void UponRestoreFingerReplyMessage(RestoreFingerReplyMessage msg, Host from, short sourceProto, int channelId) {
        logger.info("Received {} from {}", msg, from);
        setFinger(msg.getFinger(), msg.getNode());
    }

    /*------------------------------ Check Predecessor --------------------------------- */

    private void uponKeepAliveTime(KeepAliveTimer timer, long timerId) {
        if(!bIsInsideRing) return;

        dispatchMessageButNotToSelf(new KeepAliveMessage(), predecessor.getHost());
        for(Node node : fingers) {
            dispatchMessageButNotToSelf(new KeepAliveMessage(), node.getHost());
        }
    }

    private void UponKeepAliveMessage(KeepAliveMessage msg, Host from, short sourceProto, int channelId) {}

    private void uponKeepAliveMessageFail(KeepAliveMessage msg, Host host, short destProto, Throwable throwable, int channelId) {
        logger.error("Keep Alive Message {} to {} failed, reason: {}", msg, host, throwable);
        breakConnection(host);
        if(host.equals(predecessor.getHost()))
            predecessor = null;
    }

    /*----------------------------------- Search --------------------------------------- */

    public void uponLookupRequest(LookupRequest request, short sourceProto) {
        logger.info("Lookup request for {}", request.getName());
        BigInteger key = KeyGenerator.gen(request.getName(), m);
        if(ring.InBounds(key, self.getId(), getSuccessor().getId())){
            sendReply(new LookupReply(request.getRequestId(), getSuccessor()), StorageProtocol.PROTOCOL_ID);
        }
        else{
            Node closestPrecedingNode = closestPrecedingNode(key);
            if(closestPrecedingNode.equals(self))
                sendReply(new LookupReply(request.getRequestId(), getSuccessor()), StorageProtocol.PROTOCOL_ID);
            else
                dispatchMessage(new FindSuccessorMessage(request.getRequestId(), key, self.getHost()), closestPrecedingNode.getHost());
        }
    }

    private void UponFindSuccessorMessage(FindSuccessorMessage msg, Host from, short sourceProto, int channelId) {
        logger.info("Received {} from {}", msg, from);
        if(ring.InBounds(msg.getKey(), self.getId(), getSuccessor().getId())){
            dispatchMessage(new FindSuccessorReplyMessage(msg.getRequestId(), msg.getKey(), getSuccessor()), msg.getHost());
        }
        else{
            Node closestPrecedingNode = closestPrecedingNode(msg.getKey());
            if(closestPrecedingNode.equals(self))
                dispatchMessage(new FindSuccessorReplyMessage(msg.getRequestId(), msg.getKey(), getSuccessor()), msg.getHost());
            else
                dispatchMessage(msg, closestPrecedingNode.getHost());
        }
    }

    private void UponFindSuccessorReplyMessage(FindSuccessorReplyMessage msg, Host from, short sourceProto, int channelId) {
        logger.info("Received {} from {}", msg, from);
        sendReply(new LookupReply(msg.getRequestId(), msg.getSuccessor()), StorageProtocol.PROTOCOL_ID);
    }

    /*----------------------------------- Aux ---------------------------------------- */

    private Node closestPrecedingNode(BigInteger id){
        for(int i = m-1; i >= 0; i--){
            if(fingers[i] != null && ring.InBounds(fingers[i].getId(), self.getId(), id)){
                return fingers[i];
            }
        }
        return self;
    }

    /*---------------------------------------- Debug ---------------------------------- */

    private void uponInfoTime(InfoTimer timer, long timerId) {
        StringBuilder sb = new StringBuilder("Chord Metrics:\n");
        //sb.append("Membership: ").append(membership).append("\n");
        //sb.append("PendingMembership: ").append(pending).append("\n");
        sb.append(getMetrics()); //getMetrics returns an object with the number of events of each type processed by this protocol.
        logger.info(sb);
    }

    protected void uponMessageFail(ProtoMessage msg, Host host, short destProto, Throwable throwable, int channelId) {
        logger.error("Message {} to {} failed, reason: {}", msg, host, throwable);
    }
}

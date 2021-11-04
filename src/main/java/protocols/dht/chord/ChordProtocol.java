package protocols.dht.chord;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.BaseProtocol;
import protocols.dht.chord.messages.*;
import protocols.dht.chord.types.Node;
import protocols.dht.notifications.ChannelCreated;
import protocols.dht.chord.timers.FixFingersTimer;
import protocols.dht.chord.timers.InfoTimer;
import protocols.dht.chord.timers.KeepAliveTimer;
import protocols.dht.chord.timers.StabilizeTimer;
import protocols.dht.notifications.LookupResult;
import protocols.dht.requests.LookupRequest;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.channel.tcp.TCPChannel;
import pt.unl.fct.di.novasys.channel.tcp.events.*;
import pt.unl.fct.di.novasys.network.data.Host;
import utils.Ring;

import java.io.IOException;
import java.math.BigInteger;
import java.net.InetAddress;
import java.util.*;

public class ChordProtocol extends BaseProtocol {

    private static final Logger logger = LogManager.getLogger(ChordProtocol.class);

    public static final short PROTOCOL_ID = 201;
    public static final String PROTOCOL_NAME = "ChordProtocol";

    private final int m;
    private final Ring ring;

    private final Node self;
    private Node predecessor;
    private Node[] fingers;

    public ChordProtocol(Properties props, Host self) throws IOException, HandlerRegistrationException {
        super(props, self, PROTOCOL_NAME, PROTOCOL_ID, logger);

        int numberOfNodes = Integer.parseInt(props.getProperty("number_of_nodes"));
        this.m = 2*(int)(Math.log(numberOfNodes) / Math.log(2));
        this.ring = new Ring(BigInteger.TWO.pow(m));

        this.self = new Node(self,m);
        this.predecessor = null;
        this.fingers = new Node[m];

        /*--------------------- Register Request Handlers ----------------------------- */
        registerRequestHandler(LookupRequest.REQUEST_ID, this::uponLookupRequest);

        /*-------------------- Register Channel Events ------------------------------- */
        registerChannelEventHandler(channelId, OutConnectionDown.EVENT_ID, this::uponOutConnectionDown);
        registerChannelEventHandler(channelId, OutConnectionFailed.EVENT_ID, this::uponOutConnectionFailed);
        registerChannelEventHandler(channelId, OutConnectionUp.EVENT_ID, this::uponOutConnectionUp);
        registerChannelEventHandler(channelId, InConnectionUp.EVENT_ID, this::uponInConnectionUp);
        registerChannelEventHandler(channelId, InConnectionDown.EVENT_ID, this::uponInConnectionDown);

        /*--------------------- Register Timer Handlers ----------------------------- */
        registerTimerHandler(KeepAliveTimer.TIMER_ID, this::uponKeepAliveTime);
        registerTimerHandler(StabilizeTimer.TIMER_ID, this::uponStabilizeTime);
        registerTimerHandler(FixFingersTimer.TIMER_ID, this::uponFixFingersTime);
        registerTimerHandler(InfoTimer.TIMER_ID, this::uponInfoTime);

        /*---------------------- Register Message Handlers -------------------------- */
        registerMessageHandler(channelId, KeepAliveMessage.MSG_ID, this::UponKeepAliveMessage, this::uponKeepAliveMessageFail);

        registerMessageHandler(channelId, JoinRingMessage.MSG_ID, this::UponJoinRingMessage, this::uponMessageFail);
        registerMessageHandler(channelId, JoinRingReplyMessage.MSG_ID, this::UponJoinRingReplyMessage, this::uponMessageFail);

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
        buildRing(props);

        int keepAliveInterval = Integer.parseInt(props.getProperty("chord_keep_alive_interval", "4000"));
        setupPeriodicTimer(new KeepAliveTimer(), keepAliveInterval, keepAliveInterval);

        int stabilizeInterval = Integer.parseInt(props.getProperty("chord_stabilize_interval", "300"));
        setupPeriodicTimer(new StabilizeTimer(), stabilizeInterval, stabilizeInterval);

        int fixFingerInterval = Integer.parseInt(props.getProperty("chord_fix_finger_interval", "300"));
        setupPeriodicTimer(new FixFingersTimer(), fixFingerInterval, fixFingerInterval);

        int metricsInterval = Integer.parseInt(props.getProperty("protocol_metrics_interval", "1000"));
        if (metricsInterval > 0)
            setupPeriodicTimer(new InfoTimer(), metricsInterval, metricsInterval);

        //Inform the storage protocol about the channel we created in the constructor
        triggerNotification(new ChannelCreated(channelId));
    }

    private void buildRing(Properties props) {
        if(!props.containsKey("contact")) { //Create ring
            predecessor = self;
            setSuccessor(self);
            return;
        }

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

    private void UponJoinRingMessage(JoinRingMessage msg, Host from, short sourceProto, int channelId) {
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

    private void UponJoinRingReplyMessage(JoinRingReplyMessage msg, Host from, short sourceProto, int channelId) {
        logger.info("Received {} from {}", msg, from);
        setSuccessor(msg.getSuccessor());
    }


    /*--------------------------------- Stabilize -------------------------------------- */

    private void uponStabilizeTime(StabilizeTimer timer, long timerId) {
        dispatchMessage(new GetPredecessorMessage(), getSuccessor().getHost());
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
        if(nextFinger >= m) nextFinger = 1;
        BigInteger key = self.getId().add(BigInteger.TWO.pow(nextFinger));
        Node closestPrecedingNode = closestPrecedingNode(key);
        if(closestPrecedingNode.getId().compareTo(self.getId()) == 0)
            setFinger(nextFinger, getSuccessor());
        else
            dispatchMessage(new RestoreFingerMessage(nextFinger, key, self.getHost()), closestPrecedingNode.getHost());
        nextFinger++;
    }

    private void UponRestoreFingerMessage(RestoreFingerMessage msg, Host from, short sourceProto, int channelId) {
        logger.info("Received {} from {}", msg, from);
        if(ring.InBounds(msg.getKey(), self.getId(), getSuccessor().getId())){
            dispatchMessage(new RestoreFingerReplyMessage(msg.getFinger(), getSuccessor()), msg.getHost());
        }
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
        dispatchMessage(new KeepAliveMessage(), predecessor.getHost());
    }

    private void UponKeepAliveMessage(KeepAliveMessage msg, Host from, short sourceProto, int channelId) {}

    private void uponKeepAliveMessageFail(ProtoMessage msg, Host host, short destProto, Throwable throwable, int channelId) {
        logger.error("Keep Alive Message {} to {} failed, reason: {}", msg, host, throwable);
        breakConnection(predecessor.getHost());
        predecessor = null;
    }

    /*----------------------------------- Search --------------------------------------- */

    public void uponLookupRequest(LookupRequest request, short sourceProto) {
        logger.info("Lookup request for {}", request.getFullKey());
        BigInteger key = request.getFullKey().shiftRight(request.getFullKey().bitLength() - m);
        if(ring.InBounds(key, self.getId(), getSuccessor().getId())){
            triggerNotification(new LookupResult(request.getUid(), request.getFullKey(), getSuccessor()));
        }
        else{
            Node closestPrecedingNode = closestPrecedingNode(key);
            dispatchMessage(new FindSuccessorMessage(request.getUid(), request.getFullKey(), self.getHost()), closestPrecedingNode.getHost());
        }
    }

    private void UponFindSuccessorMessage(FindSuccessorMessage msg, Host from, short sourceProto, int channelId) {
        logger.info("Received {} from {}", msg, from);
        BigInteger key = msg.getKey(m);
        if(ring.InBounds(key, self.getId(), getSuccessor().getId())){
            dispatchMessage(new FindSuccessorReplyMessage(msg.getRequestId(), msg.getFullKey(), getSuccessor()), msg.getHost());
        }
        else{
            Node closestPrecedingNode = closestPrecedingNode(key);
            dispatchMessage(msg, closestPrecedingNode.getHost());
        }
    }

    private void UponFindSuccessorReplyMessage(FindSuccessorReplyMessage msg, Host from, short sourceProto, int channelId) {
        logger.info("Received {} from {}", msg, from);
        triggerNotification(new LookupResult(msg.getRequestId(), msg.getFullKey(), msg.getSuccessor()));
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
}

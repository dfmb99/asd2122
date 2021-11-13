package protocols.dht.chord;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.BaseProtocol;
import protocols.dht.chord.messages.overlay.*;
import protocols.dht.chord.messages.search.FindSuccessorMessage;
import protocols.dht.chord.messages.search.FindSuccessorReplyMessage;
import protocols.dht.chord.types.ChordKey;
import protocols.dht.chord.types.ChordNode;
import protocols.dht.chord.timers.FixFingersTimer;
import protocols.dht.chord.timers.InfoTimer;
import protocols.dht.chord.timers.KeepAliveTimer;
import protocols.dht.chord.timers.StabilizeTimer;
import protocols.dht.chord.types.ChordSegment;
import protocols.dht.replies.LookupReply;
import protocols.dht.requests.LookupRequest;
import protocols.storage.StorageProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.channel.tcp.events.*;
import pt.unl.fct.di.novasys.network.data.Host;
import protocols.dht.chord.types.Ring;

import java.io.IOException;
import java.net.InetAddress;
import java.util.*;

public class ChordProtocol extends BaseProtocol {

    private static final Logger logger = LogManager.getLogger(ChordProtocol.class);

    public static final short PROTOCOL_ID = 20;
    public static final String PROTOCOL_NAME = "ChordProtocol";

    private boolean bIsInsideRing;

    private final ChordNode self;
    private ChordNode predecessor;
    private final ChordNode[] fingers;
    private final ChordSegment[] fingerSegment;

    public ChordProtocol(Properties props, Host self) throws IOException, HandlerRegistrationException {
        super(self, PROTOCOL_NAME, PROTOCOL_ID, logger);

        int numberOfNodes = Integer.parseInt(props.getProperty("total_processes"));

        this.bIsInsideRing = false;

        int numberOfFingers = Math.min(numberOfNodes-1, 2*(int)(Math.log(numberOfNodes) / Math.log(2)));

        this.self = new ChordNode(self);
        this.predecessor = this.self;

        this.fingers = new ChordNode[numberOfFingers];
        Arrays.fill(fingers, this.self);

        this.fingerSegment = new ChordSegment[numberOfFingers];
        for(int i = 0; i<numberOfFingers; i++) {
            fingerSegment[i] = new ChordSegment(this.self.getId(), nextFinger);
        }

        /*------------------------- Create TCP Channel -------------------------------- */
        createChannel(props);

        /*---------------------- Register Channel Events ------------------------------ */
        registerChannelEventHandler(channel.id, OutConnectionDown.EVENT_ID, this::uponOutConnectionDown);
        registerChannelEventHandler(channel.id, OutConnectionFailed.EVENT_ID, this::uponOutConnectionFailed);
        registerChannelEventHandler(channel.id, OutConnectionUp.EVENT_ID, this::uponOutConnectionUp);
        registerChannelEventHandler(channel.id, InConnectionUp.EVENT_ID, this::uponInConnectionUp);
        registerChannelEventHandler(channel.id, InConnectionDown.EVENT_ID, this::uponInConnectionDown);

        /*--------------------- Register Request Handlers ----------------------------- */
        registerRequestHandler(LookupRequest.REQUEST_TYPE_ID, this::uponLookupRequest);

        /*---------------------- Register Message Handlers -------------------------- */
        registerMessageHandler(channel.id, KeepAliveMessage.MSG_ID, this::UponKeepAliveMessage, this::uponKeepAliveMessageFail);

        registerMessageHandler(channel.id, JoinRingMessage.MSG_ID, this::uponJoinRingMessage, this::uponMessageFail);
        registerMessageHandler(channel.id, JoinRingReplyMessage.MSG_ID, this::uponJoinRingReplyMessage, this::uponMessageFail);

        registerMessageHandler(channel.id, GetPredecessorMessage.MSG_ID, this::uponGetPredecessorMessage, this::uponMessageFail);
        registerMessageHandler(channel.id, GetPredecessorReplyMessage.MSG_ID, this::uponGetPredecessorReplyMessage, this::uponMessageFail);

        registerMessageHandler(channel.id, NotifySuccessorMessage.MSG_ID, this::uponNotifySuccessorMessage, this::uponMessageFail);

        registerMessageHandler(channel.id, RestoreFingerMessage.MSG_ID, this::uponRestoreFingerMessage, this::uponMessageFail);
        registerMessageHandler(channel.id, RestoreFingerReplyMessage.MSG_ID, this::uponRestoreFingerReplyMessage, this::uponMessageFail);

        registerMessageHandler(channel.id, FindSuccessorMessage.MSG_ID, this::UponFindSuccessorMessage, this::uponMessageFail);
        registerMessageHandler(channel.id, FindSuccessorReplyMessage.MSG_ID, this::UponFindSuccessorReplyMessage, this::uponMessageFail);

        /*---------------------- Register Message Serializers ---------------------- */
        registerMessageSerializer(channel.id, KeepAliveMessage.MSG_ID, KeepAliveMessage.serializer);

        registerMessageSerializer(channel.id, JoinRingMessage.MSG_ID, JoinRingMessage.serializer);
        registerMessageSerializer(channel.id, JoinRingReplyMessage.MSG_ID, JoinRingReplyMessage.serializer);

        registerMessageSerializer(channel.id, GetPredecessorMessage.MSG_ID, GetPredecessorMessage.serializer);
        registerMessageSerializer(channel.id, GetPredecessorReplyMessage.MSG_ID, GetPredecessorReplyMessage.serializer);

        registerMessageSerializer(channel.id, NotifySuccessorMessage.MSG_ID, NotifySuccessorMessage.serializer);

        registerMessageSerializer(channel.id, RestoreFingerMessage.MSG_ID, RestoreFingerMessage.serializer);
        registerMessageSerializer(channel.id, RestoreFingerReplyMessage.MSG_ID, RestoreFingerReplyMessage.serializer);

        registerMessageSerializer(channel.id, FindSuccessorMessage.MSG_ID, FindSuccessorMessage.serializer);
        registerMessageSerializer(channel.id, FindSuccessorReplyMessage.MSG_ID, FindSuccessorReplyMessage.serializer);

        /*--------------------- Register Timer Handlers ----------------------------- */
        registerTimerHandler(KeepAliveTimer.TIMER_ID, this::uponKeepAliveTime);
        registerTimerHandler(StabilizeTimer.TIMER_ID, this::uponStabilizeTime);
        registerTimerHandler(FixFingersTimer.TIMER_ID, this::uponFixFingersTime);
        registerTimerHandler(InfoTimer.TIMER_ID, this::uponInfoTime);
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
            logger.info("Joined the ring {}", self.getHost());
            return;
        }

        try {
            String contact = props.getProperty("contact");
            String[] hostElems = contact.split(":");
            Host contactHost = new Host(InetAddress.getByName(hostElems[0]), Short.parseShort(hostElems[1]));
            dispatchMessage(new JoinRingMessage(self), contactHost);
        } catch (Exception e) {
            logger.error("Invalid contact on configuration: '" + props.getProperty("contact"));
            e.printStackTrace();
            System.exit(-1);
        }
    }

    /*--------------------------------- Join Ring -------------------------------------- */

    private void uponJoinRingMessage(JoinRingMessage msg, Host from, short sourceProto, int channelId) {
        logger.info("Received {} from {}", msg, from);
        ChordNode node = msg.getNode();
        if(Ring.inBounds(node, self, getSuccessor())){
            dispatchMessage(new JoinRingReplyMessage(getSuccessor()), node.getHost());
        }
        else {
            ChordNode closestPrecedingNode = closestPrecedingNode(node);
            dispatchMessage(msg, closestPrecedingNode.getHost());
        }
    }

    private void uponJoinRingReplyMessage(JoinRingReplyMessage msg, Host from, short sourceProto, int channelId) {
        logger.info("Received {} from {}", msg, from);
        setSuccessor(msg.getNode());
        bIsInsideRing = true;
        logger.info("Joined the ring {}", self.getHost());
    }


    /*--------------------------------- Stabilize -------------------------------------- */

    private void uponStabilizeTime(StabilizeTimer timer, long timerId) {
        if(!bIsInsideRing) return;

        dispatchMessageButNotToSelf(new GetPredecessorMessage(), getSuccessor().getHost());
    }

    private void uponGetPredecessorMessage(GetPredecessorMessage msg, Host from, short sourceProto, int channelId) {
        logger.info("Received {} from {}", msg, from);
        dispatchMessage(new GetPredecessorReplyMessage(predecessor), from);
    }

    private void uponGetPredecessorReplyMessage(GetPredecessorReplyMessage msg, Host from, short sourceProto, int channelId) {
        logger.info("Received {} from {}", msg, from);
        if(Ring.inBounds(msg.getPredecessor(), self, getSuccessor()) && !ChordNode.equals(msg.getPredecessor(),self))
            setSuccessor(msg.getPredecessor());
        dispatchMessageButNotToSelf(new NotifySuccessorMessage(), getSuccessor().getHost());
    }

    private void uponNotifySuccessorMessage(NotifySuccessorMessage msg, Host from, short sourceProto, int channelId) {
        logger.info("Received {} from {}", msg, from);
        ChordNode node = new ChordNode(from);

        if(Ring.inBounds(node, predecessor, self))
            setPredecessor(node);

        if(ChordNode.equals(getSuccessor(),self))
            setSuccessor(node);
    }

    /*----------------------------------- Fix Fingers --------------------------------- */

    private int nextFinger = 1;
    private void uponFixFingersTime(FixFingersTimer timer, long timerId) {
        if(!bIsInsideRing || fingers.length == 1) return;

        if(nextFinger >= fingers.length) nextFinger = 1;
        ChordNode closestPrecedingNode = closestPrecedingNode(fingerSegment[nextFinger]);
        if(ChordNode.equals(closestPrecedingNode,self))
            setFinger(nextFinger, getSuccessor());
        else
            dispatchMessage(new RestoreFingerMessage(fingerSegment[nextFinger], self.getHost()), closestPrecedingNode.getHost());
        nextFinger++;
    }

    private void uponRestoreFingerMessage(RestoreFingerMessage msg, Host from, short sourceProto, int channelId) {
        logger.info("Received {} from {}", msg, from);
        if(Ring.inBounds(msg.getSegment(), self, getSuccessor()))
            dispatchMessage(new RestoreFingerReplyMessage(msg.getSegment(), getSuccessor()), msg.getHost());
        else {
            ChordNode closestPrecedingNode = closestPrecedingNode(msg.getSegment());
            dispatchMessage(msg, closestPrecedingNode.getHost());
        }
    }

    private void uponRestoreFingerReplyMessage(RestoreFingerReplyMessage msg, Host from, short sourceProto, int channelId) {
        logger.info("Received {} from {}", msg, from);
        if(Ring.inBounds(msg.getSegment(), self, msg.getNode()))
            setFinger(msg.getSegment().fingerIndex, msg.getNode());
    }

    /*------------------------------ Check Predecessor --------------------------------- */

    private void uponKeepAliveTime(KeepAliveTimer timer, long timerId) {
        if(!bIsInsideRing) return;

        dispatchMessageButNotToSelf(new KeepAliveMessage(), predecessor.getHost());
        for(ChordNode node : fingers) {
            dispatchMessageButNotToSelf(new KeepAliveMessage(), node.getHost());
        }
    }

    private void UponKeepAliveMessage(KeepAliveMessage msg, Host from, short sourceProto, int channelId) {}

    private void uponKeepAliveMessageFail(KeepAliveMessage msg, Host host, short destProto, Throwable throwable, int channelId) {
        logger.error("Keep Alive Message {} to {} failed, reason: {}", msg, host, throwable);
        removeHostFromView(host);
    }

    @Override
    protected void uponOutConnectionFailed(OutConnectionFailed<ProtoMessage> event, int channelId) {
        Host host = event.getNode();
        logger.info("Out Connection from {} to {} failed cause: {}", self, host, event.getCause());
        channel.openConnections.remove(host);
        channel.pendingConnections.remove(host);
        pendingMessages.remove(host);
        removeHostFromView(host);
    }

    private void removeHostFromView(Host host) {
        if(ChordNode.equals(host, fingers[fingers.length-1]))
            fingers[fingers.length-1] = self;

        for(int i=fingers.length-2; i>=0; i--) {
            if(ChordNode.equals(host, fingers[i]))
                fingers[i] = fingers[i+1];
        }

        if(ChordNode.equals(host, predecessor))
            predecessor = self;
    }



    /*----------------------------------- Search --------------------------------------- */

    public void uponLookupRequest(LookupRequest request, short sourceProto) {
        logger.info("Lookup request for {}", request.getName());
        ChordKey key = new ChordKey(request.getName());
        if(Ring.inBounds(key, self, getSuccessor())){
            sendReply(new LookupReply(request.getRequestId(), getSuccessor().toNode()), StorageProtocol.PROTOCOL_ID);
        }
        else{
            ChordNode closestPrecedingNode = closestPrecedingNode(key);
            if(ChordNode.equals(closestPrecedingNode,self))
                sendReply(new LookupReply(request.getRequestId(), getSuccessor().toNode()), StorageProtocol.PROTOCOL_ID);
            else
                dispatchMessage(new FindSuccessorMessage(request.getRequestId(), key, self.getHost()), closestPrecedingNode.getHost());
        }
    }

    private void UponFindSuccessorMessage(FindSuccessorMessage msg, Host from, short sourceProto, int channelId) {
        logger.info("Received {} from {}", msg, from);
        if(Ring.inBounds(msg.getKey(), self, getSuccessor())){
            dispatchMessage(new FindSuccessorReplyMessage(msg.getRequestId(), msg.getKey(), getSuccessor()), msg.getHost());
        }
        else{
            ChordNode closestPrecedingNode = closestPrecedingNode(msg.getKey());
            if(ChordNode.equals(closestPrecedingNode,self))
                dispatchMessage(new FindSuccessorReplyMessage(msg.getRequestId(), msg.getKey(), getSuccessor()), msg.getHost());
            else
                dispatchMessage(msg, closestPrecedingNode.getHost());
        }
    }

    private void UponFindSuccessorReplyMessage(FindSuccessorReplyMessage msg, Host from, short sourceProto, int channelId) {
        logger.info("Received {} from {}", msg, from);
        sendReply(new LookupReply(msg.getRequestId(), msg.getSuccessor().toNode()), StorageProtocol.PROTOCOL_ID);
    }

    /*----------------------------------- Aux ---------------------------------------- */

    private ChordNode closestPrecedingNode(ChordNode node){
        return closestPrecedingNode(node.getId());
    }

    private ChordNode closestPrecedingNode(ChordSegment segment) {
        return closestPrecedingNode(new ChordKey(segment));
    }

    private ChordNode closestPrecedingNode(ChordKey key){
        for(int i = fingers.length-1; i >= 0; i--){
            if(!fingers[i].equals(self) && Ring.inBounds(key, fingers[i], self)){
                return fingers[i];
            }
        }
        return self;
    }

    private ChordNode getSuccessor() {
        return fingers[0];
    }

    private void setSuccessor(ChordNode successor) {
        setFinger(0, successor);
    }

    private void setPredecessor(ChordNode predecessor) {
        if(ChordNode.equals(predecessor,this.predecessor)) return;

        if(channel.decrementConnectionUses(this.predecessor.getHost()) == 0)
            breakConnection(this.predecessor.getHost());

        this.predecessor = predecessor;

        if(!ChordNode.equals(predecessor,self)) {
            mendConnection(predecessor.getHost());
            channel.incrementConnectionUses(predecessor.getHost());
        }
    }

    private void setFinger(int i, ChordNode finger) {
        if(ChordNode.equals(finger,this.fingers[i])) return;

        if(channel.decrementConnectionUses(this.fingers[i].getHost()) == 0)
            breakConnection(this.fingers[i].getHost());

        this.fingers[i] = finger;

        if(!ChordNode.equals(finger,self)) {
            mendConnection(finger.getHost());
            channel.incrementConnectionUses(finger.getHost());
        }
    }

    /*---------------------------------------- Debug ---------------------------------- */

    private void uponInfoTime(InfoTimer timer, long timerId) {
        StringBuilder sb = new StringBuilder("Chord Metrics:\n");
        sb.append("Predecessor: ").append(predecessor).append("\n");
        sb.append("Successor: ").append(getSuccessor()).append("\n");
        //sb.append(getMetrics()); //getMetrics returns an object with the number of events of each type processed by this protocol.
        logger.info(sb);
    }

    protected void uponMessageFail(ProtoMessage msg, Host host, short destProto, Throwable throwable, int channelId) {
        logger.error("Message {} to {} failed, reason: {}", msg, host, throwable);
    }
}

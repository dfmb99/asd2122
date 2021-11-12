package protocols.dht.kademlia;

import notifications.ChannelCreated;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.BaseProtocol;
import protocols.dht.chord.timers.InfoTimer;
import protocols.dht.kademlia.messages.FindNodeMessage;
import protocols.dht.kademlia.messages.FindNodeReplyMessage;
import protocols.dht.kademlia.messages.PingMessage;
import protocols.dht.kademlia.messages.PingReplyMessage;
import protocols.dht.kademlia.timers.PingTimer;
import protocols.dht.kademlia.types.KademliaNode;
import protocols.dht.kademlia.types.Node;
import protocols.dht.requests.LookupRequest;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.channel.tcp.events.*;
import pt.unl.fct.di.novasys.network.data.Host;
import utils.HashGenerator;

import java.io.IOException;
import java.math.BigInteger;
import java.net.InetAddress;
import java.util.*;

public class KademliaProtocol extends BaseProtocol {

    private static final Logger logger = LogManager.getLogger(KademliaProtocol.class);

    public static final short PROTOCOL_ID = 30;
    public static final String PROTOCOL_NAME = "KademliaProtocol";
    public static final int BIT_SPACE = 160;


    private final int k;
    private final int alfa;
    private final int beta;
    private final int pingTimeout;

    private final Node self;
    private final List<List<Node>> routingTable;

    private final Map<BigInteger, Set<Host>> currentQueries; // map of current alfa nodes we are waiting for a findNodeReply
    private final Map<BigInteger, Set<Host>> finishedQueries; // map of queries already performed
    private final Map<BigInteger, SortedSet<KademliaNode>> currentClosestK; // map of current list of closest k nodes
    private final Map<BigInteger, Integer> receivedReplies; // keeps track of the number of received replies to know if we already got beta replies

    private final Map<Double, Node> pingPendingToLeave; // keeps track of nodes waiting for a ping reply/fail to leave our kbuckets
    private final Map<Double, Node> pingPendingToEnter; // keeps track of nodes waiting for a ping reply/fail to enter our kbuckets





    public KademliaProtocol(Properties props, Host self) throws IOException, HandlerRegistrationException {
        super(self, PROTOCOL_NAME, PROTOCOL_ID, logger);

        this.k = Integer.parseInt(props.getProperty("k_value"));
        this.alfa = Integer.parseInt(props.getProperty("alfa_value"));
        this.beta = Integer.parseInt(props.getProperty("beta_value"));
        this.pingTimeout = Integer.parseInt(props.getProperty("ping_timeout"));

        this.self = new Node(self);
        this.routingTable = new ArrayList<>(BIT_SPACE);

        this.currentQueries     = new HashMap<>();
        this.finishedQueries    = new HashMap<>();
        this.currentClosestK    = new HashMap<>();
        this.receivedReplies    = new HashMap<>();

        this.pingPendingToLeave = new HashMap<>();
        this.pingPendingToEnter = new HashMap<>();

        /*------------------------- Create TCP Channel -------------------------------- */
        createChannel(props);

        /*---------------------- Register Channel Events ------------------------------ */
        registerChannelEventHandler(channel.id, OutConnectionDown.EVENT_ID, this::uponOutConnectionDown);
        registerChannelEventHandler(channel.id, OutConnectionFailed.EVENT_ID, this::uponOutConnectionFailed);
        registerChannelEventHandler(channel.id, OutConnectionUp.EVENT_ID, this::uponOutConnectionUp);
        registerChannelEventHandler(channel.id, InConnectionUp.EVENT_ID, this::uponInConnectionUp);
        registerChannelEventHandler(channel.id, InConnectionDown.EVENT_ID, this::uponInConnectionDown);

        /*----------------------- Register Request Handlers --------------------------- */
        registerRequestHandler(LookupRequest.REQUEST_TYPE_ID, this::uponLookupRequest);

        /*----------------------- Register Message Handlers --------------------------- */
        registerMessageHandler(channel.id, FindNodeMessage.MSG_ID, this::uponFindNodeMessage, this::uponMessageFail);
        registerMessageHandler(channel.id, FindNodeReplyMessage.MSG_ID, this::uponFindNodeReplyMessage, this::uponMessageFail);
        registerMessageHandler(channel.id, PingMessage.MSG_ID, this::uponPingMessage, this::uponMessageFail);
        registerMessageHandler(channel.id, PingReplyMessage.MSG_ID, this::uponPingReplyMessage, this::uponMessageFail);

        /*------------------------ Register Timers Handler ---------------------------- */
        registerTimerHandler(PingTimer.TIMER_ID, this::uponPingTimer);

    }

    @Override
    public void init(Properties props) {
        buildRoutingTable(props);

        //int metricsInterval = Integer.parseInt(props.getProperty("protocol_metrics_interval", "1000"));
        //if (metricsInterval > 0)
        //    setupPeriodicTimer(new InfoTimer(), metricsInterval, metricsInterval);

        triggerNotification(new ChannelCreated(channel));

        logger.info("Hello, I am {}", self);
    }


    public void uponLookupRequest(LookupRequest request, short sourceProto) {
        logger.info("Lookup request for {}", request.getName());

        BigInteger id = HashGenerator.generateHash(request.getName());
        if(currentQueries.get(id) != null)  // If we are already performing a lookup for that id
            return;                         // we do nothing here

        currentClosestK.put(id, findKClosestNodes(id));
        currentQueries.put(id, new TreeSet<>());
        finishedQueries.put(id, new TreeSet<>());
        receivedReplies.put(id, 0);

        FindNodeMessage msg = new FindNodeMessage(id);
        Iterator<KademliaNode> it = currentClosestK.get(id).iterator();
        for(int i = 0; (i < alfa) && it.hasNext(); i++){  // accounts for case where we don't have k nodes in our kbuckets
            KademliaNode recipient = it.next();
            dispatchMessage(msg, recipient.getHost());
            Set<Host> currQueries = currentQueries.get(id);
            currQueries.add(recipient.getHost());
        }

    }

    private void uponFindNodeMessage(FindNodeMessage msg, Host from, short sourceProto, int channelId) {
        logger.info("Received {} from {}", msg, from);

        SortedSet<KademliaNode> closestK = findKClosestNodes(msg.getLookUpId());
        updateRoutingTable(from);
        dispatchMessage(new FindNodeReplyMessage(msg.getLookUpId(), closestK), from);
    }


    private void uponFindNodeReplyMessage(FindNodeReplyMessage msg, Host from, short sourceProto, int channelId) {
        logger.info("Received {} from {}", msg, from);

        BigInteger id = msg.getLookupId();
        SortedSet<KademliaNode> peerClosestK = msg.getClosestNodes();

        if(currentClosestK.get(id) == null) // reply relative to a lookUp req. already terminated
            return;

        Integer totalReceivedReplies = receivedReplies.get(id);
        totalReceivedReplies++;

        Set<Host> currentQueries = this.currentQueries.get(id);
        currentQueries.remove(from);

        Set<Host> finishedQueries = this.finishedQueries.get(id);
        finishedQueries.add(from);

        SortedSet<KademliaNode> currentClosestK = this.currentClosestK.get(id);
        currentClosestK.addAll(peerClosestK);

        if(totalReceivedReplies % beta == 0){
            List<KademliaNode> closest = new LinkedList<>(currentClosestK);
            closest.sort(null);
            currentClosestK = new TreeSet<>(closest.subList(0, Math.min(k, closest.size())));

            if(allClosestWereQueried(currentClosestK, finishedQueries)){
                //sendReply(); TODO: finish sending the reply to the StorageProtocol - alterar classe LookupReply para passar uma lista/set (kbuckets)
            }
        }

        for(int i = 0; i < alfa - currentQueries.size(); i++){
            FindNodeMessage findNode = new FindNodeMessage(id);
            Host peer = firstNotQueried(currentClosestK, finishedQueries);
            assert peer != null;
            dispatchMessage(findNode, peer);
            currentQueries.add(peer);
        }

        updateRoutingTable(from);
    }

    private void uponPingMessage(PingMessage msg, Host from, short sourceProto, int channelId){
        logger.info("Received {} from {}", msg, from);

        dispatchMessage(new PingReplyMessage(msg.getUid()), from);
    }

    private void uponPingReplyMessage(PingReplyMessage msg, Host from, short sourceProto, int channelId){
        logger.info("Received {} from {}", msg, from);

        pingPendingToEnter.remove(msg.getUid());
        pingPendingToLeave.remove(msg.getUid());
    }

    private void uponPingTimer(PingTimer timer, long timerId){
        logger.info("Ping {} timed out", timer);

        Double pingUid = timer.getPingUid();
        Node enteringNode = pingPendingToEnter.remove(pingUid);
        Node leavingNode = pingPendingToLeave.remove(pingUid);

        List<Node> bucket = findBucket(HashGenerator.generateHash(enteringNode.getHost().toString()));
        bucket.remove(leavingNode);
        bucket.add(enteringNode);
    }


    protected void uponMessageFail(ProtoMessage msg, Host host, short destProto, Throwable throwable, int channelId) {
        logger.error("Message {} to {} failed, reason: {}", msg, host, throwable);
    }



    /*----------------------------------- Aux ---------------------------------------- */


    private void buildRoutingTable(Properties props) {

        int myBucket = findBucketIndex(self.getId());
        List<Node> kBucket;
        for (int i = 0; i < BIT_SPACE; i++) {
            kBucket = new ArrayList<>(k);
            if(i == myBucket)
                kBucket.add(self);
            routingTable.add(kBucket);
        }

        if (props.containsKey("contact")) {
            try {
                String contact = props.getProperty("contact");
                String[] hostElems = contact.split(":");
                Node contactNode = new Node(new Host(InetAddress.getByName(hostElems[0]), Integer.parseInt(hostElems[1])));
                BigInteger contactId = HashGenerator.generateHash(contactNode.getHost().toString());
                findBucket(contactId).add(contactNode);
                dispatchMessage(new FindNodeMessage(self.getId()), contactNode.getHost());
            } catch (Exception e) {
                logger.error("Invalid contact on configuration: '" + props.getProperty("contact"));
                e.printStackTrace();
                System.exit(-1);
            }
        }

    }

    private void updateRoutingTable(Host p){

        Node peer = new Node(p);
        List<Node> bucket = findBucket(peer.getId());

        if(bucket.contains(peer)){
            bucket.remove(peer);
            bucket.add(peer);
        }
        else{
            if(bucket.size() < k){
                bucket.add(peer);
            }
            else{ // bucket full
                Node oldest = bucket.get(0); // get the head/oldest
                Double pingUid = Math.random();
                dispatchMessage(new PingMessage(pingUid), oldest.getHost());
                pingPendingToLeave.put(pingUid, oldest);
                pingPendingToEnter.put(pingUid, peer);
                setupTimer(new PingTimer(pingUid), pingTimeout);
            }
        }

    }

    private List<Node> findBucket(BigInteger id) {
        int idx = findBucketIndex(id);
        return routingTable.get(idx);
    }

    private int findBucketIndex(BigInteger id) {
        return (int) Math.floor(Math.log(id.doubleValue()));
    }




    private Host firstNotQueried(SortedSet<KademliaNode> closestK, Set<Host> finishedQueries){
        Iterator<KademliaNode> it = closestK.iterator();
        KademliaNode curr;
        Host currHost;
        while(it.hasNext()){
            curr = it.next();
            currHost = curr.getHost();
            if( !finishedQueries.contains(currHost) )
                return currHost;
        }
        return null; // shouldn't happen if implementation is correct
    }

    private SortedSet<KademliaNode> findKClosestNodes(BigInteger id){
        int bucketIdx = findBucketIndex(id);
        List<Node> bucket = routingTable.get(bucketIdx);
        List<KademliaNode> closeNodes = new LinkedList<>();

        for (Node n : bucket)
            closeNodes.add(new KademliaNode(n.getHost(), id));

        int round = 1;
        int bucketBelowIdx = bucketIdx - round;
        int bucketAboveIdx = bucketIdx + round;

        while(closeNodes.size() < k){

            if(bucketBelowIdx >= 0){
                List<Node> bucketBelow = routingTable.get(bucketBelowIdx);
                for(Node n : bucketBelow)
                    closeNodes.add(new KademliaNode(n.getHost(), id));
            }

            if(bucketAboveIdx < 160){
                List<Node> bucketAbove = routingTable.get(bucketAboveIdx);
                for(Node n : bucketAbove)
                    closeNodes.add(new KademliaNode(n.getHost(), id));
            }

            round++;
            bucketBelowIdx--;
            bucketAboveIdx++;

            if(bucketBelowIdx < 0 && bucketAboveIdx >= 160)
                break;
        }

        closeNodes.sort(null);
        return new TreeSet<>(closeNodes.subList(0, Math.min(k, closeNodes.size())));
    }

    private boolean allClosestWereQueried(SortedSet<KademliaNode> currentClosestK, Set<Host> finishedQueries){
        for (KademliaNode n : currentClosestK) {
            if(!finishedQueries.contains(n.getHost()))
                return false;
        }
        return true;
    }


}

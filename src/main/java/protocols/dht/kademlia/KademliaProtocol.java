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

    private final Node self;
    private final List<List<Node>> routingTable;

    private final Map<BigInteger, Set<Host>> currentQueries; // map of current alfa nodes we are waiting for a findNodeReply
    private final Map<BigInteger, Set<Host>> finishedQueries; // map of queries already performed
    private final Map<BigInteger, SortedSet<KademliaNode>> currentClosestK; // map of current list of closest k nodes
    private final Map<BigInteger, Integer> receivedReplies; // keeps track of the number of received replies to know if we already got beta replies

    private final Map<Double, BigInteger> pingPendingToLeave; // keeps track of nodes waiting for a ping reply/fail to leave our kbuckets
    private final Map<Double, BigInteger> pingPendingToEnter; // keeps track of nodes waiting for a ping reply/fail to enter our kbuckets





    public KademliaProtocol(Properties props, Host self) throws IOException, HandlerRegistrationException {
        super(self, PROTOCOL_NAME, PROTOCOL_ID, logger);

        this.k = Integer.parseInt(props.getProperty("k_value"));
        this.alfa = Integer.parseInt(props.getProperty("alfa_value"));
        this.beta = Integer.parseInt(props.getProperty("beta_value"));

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

        /*--------------------- Register Request Handlers ----------------------------- */
        registerRequestHandler(LookupRequest.REQUEST_TYPE_ID, this::uponLookupRequest);

        /*---------------------- Register Message Handlers -------------------------- */
        registerMessageHandler(channel.id, FindNodeMessage.MSG_ID, this::UponFindNodeMessage, this::uponMessageFail);
        registerMessageHandler(channel.id, FindNodeReplyMessage.MSG_ID, this::UponFindNodeReplyMessage, this::uponMessageFail);
        registerMessageHandler(channel.id, PingMessage.MSG_ID, this::UponPingMessage, this::uponMessageFail);
        registerMessageHandler(channel.id, PingMessage.MSG_ID, this::UponPingReplyMessage, this::uponMessageFail);

        // TODO: register ping timeout handler

    }

    @Override
    public void init(Properties props) {
        buildRoutingTable(props);

        int metricsInterval = Integer.parseInt(props.getProperty("protocol_metrics_interval", "1000"));
        if (metricsInterval > 0)
            setupPeriodicTimer(new InfoTimer(), metricsInterval, metricsInterval);

        triggerNotification(new ChannelCreated(channel));
    }


    public void uponLookupRequest(LookupRequest request, short sourceProto) {
        logger.info("Lookup request for {}", request.getName());

        BigInteger id = HashGenerator.generateHash(request.getName());
        if(currentQueries.get(id) != null)  // If we are already performing a lookup for that id
            return;                         // we do nothing here

        currentClosestK.put(id, findKClosestNodes(id));
        currentQueries.put(id, new TreeSet<Host>());
        finishedQueries.put(id, new TreeSet<Host>());
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

    private void UponFindNodeMessage(FindNodeMessage msg, Host from, short sourceProto, int channelId) {
        logger.info("Received {} from {}", msg, from);

        BigInteger lookUpId = msg.getLookUpId();
        SortedSet<KademliaNode> closestK = findKClosestNodes(lookUpId);

        updateRoutingTable(from);

        dispatchMessage(new FindNodeReplyMessage(lookUpId, closestK), from);
    }


    private void UponFindNodeReplyMessage(FindNodeReplyMessage msg, Host from, short sourceProto, int channelId) {
        logger.info("Received {} from {}", msg, from);

        BigInteger id = msg.getLookupId();
        SortedSet<KademliaNode> peerClosestK = msg.getClosestNodes();

        if(currentClosestK.get(id) == null)    // reply relative to a lookUp req. already terminated
            return;

        Integer totalReceivedReplies = receivedReplies.get(id);
        totalReceivedReplies++;

        Set<Host> currentQueries = this.currentQueries.get(id);
        currentQueries.remove(from);

        Set<Host> finishedQueries = this.finishedQueries.get(id);
        finishedQueries.add(from);

        SortedSet<KademliaNode> currentClosestK = this.currentClosestK.get(id);
        boolean closestKChanged = currentClosestK.addAll(peerClosestK);

        if(totalReceivedReplies % beta == 0){
            if(!closestKChanged){
                //sendReply(); TODO: finish sending the reply to the StorageProtocol - alterar classe LookupReply para passar uma lista/set (kbuckets)
            }
            currentClosestK = reduceToKElements(currentClosestK);
        }

        for(int i = 0; i < alfa - currentQueries.size(); i++){
            FindNodeMessage findNode = new FindNodeMessage(id);
            Host peer = firstNotQueried(currentClosestK, finishedQueries);
            if(peer == null)
                break;
            dispatchMessage(findNode, peer);
            currentQueries.add(peer);
        }

        updateRoutingTable(from);

    }

    private void UponPingMessage(PingMessage msg, Host from, short sourceProto, int channelId){
        dispatchMessage(new PingReplyMessage(msg.getUid()), from);
    }

    private void UponPingReplyMessage(PingReplyMessage msg, Host from, short sourceProto, int channelId){
        BigInteger peerId = HashGenerator.generateHash(from.toString());
        List<Node> bucket = findBucket(peerId);
        // TODO: finnish
    }



    protected void uponMessageFail(ProtoMessage msg, Host host, short destProto, Throwable throwable, int channelId) {
        logger.error("Message {} to {} failed, reason: {}", msg, host, throwable);
    }



    /*----------------------------------- Aux ---------------------------------------- */


    private void buildRoutingTable(Properties props) {
        List<Node> kbucket;
        for (int i = 0; i < BIT_SPACE; i++) {
            kbucket = new ArrayList<>(k);
            routingTable.add(kbucket);
        }

        if (props.containsKey("contact")) {
            try {
                String contact = props.getProperty("contact");
                String[] hostElems = contact.split(":");
                Node contactNode = new Node(new Host(InetAddress.getByName(hostElems[0]), Integer.parseInt(hostElems[1])));
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
                bucket.remove(peer);
                bucket.add(peer);
            }
            else{
                Node oldest = bucket.get(0); // get the head/oldest
                Double pingUid = Math.random();
                dispatchMessage(new PingMessage(pingUid), oldest.getHost());
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

    private Node[] findAlfaClosestNodes(BigInteger id) {
        List<Node> bucket = findBucket(id);
        int resSize = Math.min(bucket.size(), alfa);
        List<Node> alreadyContainedNodes = new LinkedList<>();


        Node closest = null;
        BigInteger closestDist = null;
        for(int i = 0; i < resSize; i++){               // Go over the bucket alfa (or bucket.size if bucket.size < alfa) times
            for(int j = 0; j < bucket.size(); j++){     // Store the closest node that we don't already have at each cicle
                Node current = bucket.get(j);
                if(j == 0){ // initialize closest and closestDist
                    for(int k = 0; k < alfa; k++){
                        Node curr = bucket.get(k);
                        if (!alreadyContainedNodes.contains(curr)) {
                            closest = curr;
                            closestDist = getDistance(id, curr.getId());
                            break;
                        }
                    }
                }
                BigInteger dist = getDistance(current.getId(), id);
                if( ( dist.compareTo(closestDist) < 0 ) && ( !alreadyContainedNodes.contains(closest) ) ){
                    closest = current;
                    closestDist = dist;
                }
            }
            alreadyContainedNodes.add(closest);
        }

        Node[] closestNodes = new Node[resSize];
        for(int i = 0; i < resSize; i++){
            closestNodes[i] = alreadyContainedNodes.get(i);
        }
        return closestNodes;
    }

    /**
     *  Gets distance between two nodes
     * @param id1 - id of node1
     * @param id2 - id of node2
     */
    private BigInteger getDistance(BigInteger id1, BigInteger id2) {
        return id1.xor(id2);
    }


    private List<Node> getClosestList(BigInteger lookUpId, List<Node> bucket, int idx) {
        List<Node> closest = new ArrayList<>();
        // updates closest nodes with current bucket
        updateClosestList(lookUpId, bucket, closest);

        int i = 1;
        while (closest.size() < this.k && ( idx - i >= 0 || idx + i < this.routingTable.size() - 1)) {
            if (idx - i >= 0) {
                List<Node> l1 = this.routingTable.get(idx - i);
                updateClosestList(lookUpId, l1, closest);
            }
            if (idx + i < this.routingTable.size() - 1) {
                List<Node> l2 = this.routingTable.get(idx + i);
                updateClosestList(lookUpId, l2, closest);
            }
            i++;
        }
        return closest;
    }

    private void updateClosestList(BigInteger lookUpId, List<Node> bucket, List<Node> closest) {
        for (Node n : bucket) {
            if (closest.size() < this.k) {
                closest.add(n);
            } else {
                Node biggestDistNode = getBiggestDistance(lookUpId, closest);
                BigInteger biggestDist = getDistance(lookUpId, biggestDistNode.getId());
                BigInteger currDist = getDistance(lookUpId, n.getId());
                if (currDist.compareTo(biggestDist) < 0) {
                    closest.remove(biggestDistNode);
                    closest.add(n);
                }
            }
        }
    }

    // returns the node from the list with the biggest distance to id

    private Node getBiggestDistance(BigInteger id, List<Node> list) {
        Node biggestDistNode = null;
        BigInteger maxDist = BigInteger.ZERO;
        for (Node n : list) {
            BigInteger curr = getDistance(id, n.getId());
            if (curr.compareTo(maxDist) > 0) {
                biggestDistNode = n;
                maxDist = curr;
            }
        }
        return biggestDistNode;
    }

    private SortedSet<KademliaNode> reduceToKElements(SortedSet<KademliaNode> closestK){

        if(closestK.size() - k > 0){
            SortedSet<KademliaNode> closestKCopy = new TreeSet<>();
            Iterator<KademliaNode> currIt = closestK.iterator();
            KademliaNode currNode;
            for(int i = 0; i < k; i++){
                currNode = currIt.next();
                closestKCopy.add(currNode);
            }
            return closestKCopy;
        }
        else{
            return closestK;
        }
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


}

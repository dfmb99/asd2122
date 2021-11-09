package protocols.dht.kademlia;

import notifications.ChannelCreated;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.BaseProtocol;
import protocols.dht.chord.timers.InfoTimer;
import protocols.dht.kademlia.messages.FindNodeMessage;
import protocols.dht.kademlia.messages.FindNodeReplyMessage;
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


    private final Map<BigInteger, SortedSet<KademliaNode>> currentQueries;      // map of current alfa nodes we are waiting for a findNodeReply
    private final Map<BigInteger, SortedSet<KademliaNode>> finishedQueries;     // map of queries already performed
    private final Map<BigInteger, SortedSet<KademliaNode>>  currentClosestK;     // map of current list of closest k nodes





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
        currentQueries.put(id, new TreeSet<KademliaNode>());
        finishedQueries.put(id, new TreeSet<KademliaNode>());

        FindNodeMessage msg = new FindNodeMessage(id);
        Iterator<KademliaNode> it = currentClosestK.get(id).iterator();
        for(int i = 0; (i < alfa) && it.hasNext(); i++){  // accounts for case where we don't have k nodes in our kbuckets
            KademliaNode recipient = it.next();
            dispatchMessage(msg, recipient.getHost());
            SortedSet<KademliaNode> currQueries = currentQueries.get(id);
            currQueries.add(recipient);
        }

    }

    private void UponFindNodeMessage(FindNodeMessage msg, Host from, short sourceProto, int channelId) {
        logger.info("Received {} from {}", msg, from);

        BigInteger lookUpId = msg.getLookUpId();
        int idx = findBucketIndex(lookUpId);
        List<Node> bucket = findBucket(lookUpId);
        Node peer = new Node(from);

        // if bucket contains peers removes from the list and adds it at the end
        if (bucket.contains(peer)) {
            bucket.remove(peer);
            bucket.add(peer);
        }

        List<Node> closest = getClosestList(lookUpId, bucket, idx);
        dispatchMessage(new FindNodeReplyMessage(closest.toArray(new Node[0])), from);
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

    private void UponFindNodeReplyMessage(FindNodeReplyMessage msg, Host from, short sourceProto, int channelId) {
        logger.info("Received {} from {}", msg, from);

    }

    protected void uponMessageFail(ProtoMessage msg, Host host, short destProto, Throwable throwable, int channelId) {
        logger.error("Message {} to {} failed, reason: {}", msg, host, throwable);
    }


    /*----------------------------------- Aux ---------------------------------------- */

    private void buildRoutingTable(Properties props) {
        List<Node> kbucket;
        for (int i = 0; i < BIT_SPACE; i++) {
            kbucket = new ArrayList<Node>(k);
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

    private List<Node> findBucket(BigInteger id) {
        int idx = findBucketIndex(id);
        return routingTable.get(idx);
    }

    private int findBucketIndex(BigInteger id) {
        return (int) Math.floor(Math.log(id.doubleValue()));
    }

    private Node[] findAlfaClosestNodes(BigInteger id) { // TODO: debug, check if behaves properly
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

}

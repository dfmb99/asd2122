package protocols.dht.kademlia;

import notifications.ChannelCreated;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.BaseProtocol;
import protocols.dht.chord.KeyGenerator;
import protocols.dht.chord.timers.InfoTimer;
import protocols.dht.kademlia.Timers.NoLookUpReplyTimer;
import protocols.dht.kademlia.messages.FindNodeMessage;
import protocols.dht.kademlia.messages.FindNodeReplyMessage;
import protocols.dht.kademlia.types.Node;
import protocols.dht.requests.LookupRequest;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.channel.tcp.events.*;
import pt.unl.fct.di.novasys.network.data.Host;

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

    private final Node self;
    private final List<List<Node>> routingTable;


    private final Map<Integer, List<Node>> currentLookups;  // map of nodes involved in the lookup process atm (waiting for a findNodeReply)
    private final Map<BigInteger, Integer> lookupOperations;  // k-id we are trying to find, v-uniqueId

    private final long lookUpTimeOut;



    public KademliaProtocol(Properties props, Host self) throws IOException, HandlerRegistrationException {
        super(self, PROTOCOL_NAME, PROTOCOL_ID, logger);

        this.k = Integer.parseInt(props.getProperty("k_value"));
        this.alfa = Integer.parseInt(props.getProperty("alfa_value"));

        this.self = new Node(self);
        this.routingTable = new ArrayList<>(BIT_SPACE);
        this.currentLookups = new HashMap<>();
        this.lookupOperations = new HashMap<>();

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

        registerTimerHandler(NoLookUpReplyTimer.TIMER_ID, this::uponNoLookUpReplyTimer);
        this.lookUpTimeOut = Integer.parseInt(props.getProperty("find_node_timeout"));
    }

    @Override
    public void init(Properties props) {
        buildRoutingTable(props);

        int metricsInterval = Integer.parseInt(props.getProperty("protocol_metrics_interval", "1000"));
        if (metricsInterval > 0)
            setupPeriodicTimer(new InfoTimer(), metricsInterval, metricsInterval);

        triggerNotification(new ChannelCreated(channelId));
    }

    public void uponLookupRequest(LookupRequest request, short sourceProto) {
        logger.info("Lookup request for {}", request.getName());

        BigInteger id = KeyGenerator.gen(request.getName(), BIT_SPACE);
        List<Node> bucket = findBucket(id);
        Node[] alfaClosestNodes = findAlfaClosestNodes(id);
        FindNodeMessage msg = new FindNodeMessage(id);
        for (Node alfaClosestNode : alfaClosestNodes) {
            sendMessage(msg, alfaClosestNode.getHost());
            setupTimer(new NoLookUpReplyTimer(), lookUpTimeOut);
        }

    }

    private void UponFindNodeMessage(FindNodeMessage msg, Host from, short sourceProto, int channelId) {
        logger.info("Received {} from {}", msg, from);

        List<Node> bucket = findBucket(msg.getLookUpId());
        Node peer = new Node(from);

        if(bucket.contains(peer)){
            bucket.remove(peer);
            bucket.add(peer);
        }
        else{
            if(bucket.size() < 20){
                bucket.add(peer);
            }
            else{   // bucket if full (size == 20)
                /**
                 * TODO:
                 *  - create ping message
                 *  - create ping message reply
                 *  - create a ping timer
                 *  - create data structure in order to keep running pings
                 *      to know when to insert a new node to the full bucket
                 */
            }
        }

        // TODO: reply with k closest nodes i know




    }

    private void UponFindNodeReplyMessage(FindNodeReplyMessage msg, Host from, short sourceProto, int channelId) {
        logger.info("Received {} from {}", msg, from);

        Node peer = new Node(from);
        boolean known = false;
        for(Bucket bucket : routingTable){
            if(bucket.containsNode(peer)){
                known = true;
            }
        }

        if(known == false)
    }

    protected void uponNoLookUpReplyTimer(NoLookUpReplyTimer timer, long timerId) {

    }

    protected void uponMessageFail(ProtoMessage msg, Host host, short destProto, Throwable throwable, int channelId) {
        logger.error("Message {} to {} failed, reason: {}", msg, host, throwable);
    }


    /*----------------------------------- Aux ---------------------------------------- */

    /**
     *  Gets distance between two nodes
     * @param id1 - id of node1
     * @param id2 - id of node2
     */
    private BigInteger getDistance(BigInteger id1, BigInteger id2) {
        return id1.xor(id2);
    }

    private void buildRoutingTable(Properties props) {
        List<Node> kbucket;
        for(int i = 0; i < BIT_SPACE; i++){
            kbucket = new ArrayList<Node>(k);
            routingTable.add(kbucket);
        }

        if(props.containsKey("contact")) {
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

    private List<Node> findBucket(BigInteger id){
        int idx = findBucketIndex(id);
        return routingTable.get(idx);
    }

    private int findBucketIndex(BigInteger id){
        return (int) Math.floor(Math.log(id.doubleValue()));
    }

    private Node[] findAlfaClosestNodes(BigInteger id){
        List<Node> bucket = findBucket(id);
        int resSize = Math.min(bucket.size(), alfa);
        Node[] closestNodes = new Node[resSize];
        for(int i = 0; i < resSize; i++){
            closestNodes[i] = bucket.get(bucket.size() - i - 1);
        }
        return closestNodes;
    }

}

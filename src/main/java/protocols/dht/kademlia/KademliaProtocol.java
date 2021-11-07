package protocols.dht.kademlia;

import notifications.ChannelCreated;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.BaseProtocol;
import protocols.dht.chord.timers.InfoTimer;
import protocols.dht.kademlia.messages.FindNodeMessage;
import protocols.dht.kademlia.messages.FindNodeReplyMessage;
import protocols.dht.kademlia.types.Bucket;
import protocols.dht.kademlia.types.Node;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.math.BigInteger;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

public class KademliaProtocol extends BaseProtocol {

    private static final Logger logger = LogManager.getLogger(KademliaProtocol.class);

    public static final short PROTOCOL_ID = 30;
    public static final String PROTOCOL_NAME = "KademliaProtocol";
    private static final int K = 20;

    private final Node self;
    private List<Bucket> routingTable;


    public KademliaProtocol(Properties props, Host self) throws IOException, HandlerRegistrationException {
        super(props, self, PROTOCOL_NAME, PROTOCOL_ID, logger, true);

        this.self = new Node(self);
        this.routingTable = new ArrayList<>();

        /*---------------------- Register Channel Events ------------------------------ */
        registerChannelEvents();

        /*---------------------- Register Message Handlers -------------------------- */
        registerMessageHandler(channelId, FindNodeMessage.MSG_ID, this::UponFindNodeMessage, this::uponMessageFail);
        registerMessageHandler(channelId, FindNodeReplyMessage.MSG_ID, this::UponFindNodeReplyMessage, this::uponMessageFail);
    }

    @Override
    public void init(Properties props) {
        buildRoutingTable(props);

        int metricsInterval = Integer.parseInt(props.getProperty("protocol_metrics_interval", "1000"));
        if (metricsInterval > 0)
            setupPeriodicTimer(new InfoTimer(), metricsInterval, metricsInterval);

        triggerNotification(new ChannelCreated(channelId));
    }

    private void buildRoutingTable(Properties props) {
        if(!props.containsKey("contact")) {
            // if this node is alone on the protocol does nothing
            return;
        }
        try {
            String contact = props.getProperty("contact");
            String[] hostElems = contact.split(":");
            Node contactNode = new Node(new Host(InetAddress.getByName(hostElems[0]), Short.parseShort(hostElems[1])));
            dispatchMessage(new FindNodeMessage(self.getId()), contactNode.getHost());
        } catch (Exception e) {
            logger.error("Invalid contact on configuration: '" + props.getProperty("contact"));
            e.printStackTrace();
            System.exit(-1);
        }
    }

    private void UponFindNodeMessage(FindNodeMessage msg, Host from, short sourceProto, int channelId) {
        logger.info("Received {} from {}", msg, from);
        if (routingTable.size() == 0) {
            dispatchMessage(new FindNodeReplyMessage(self), from);
            Bucket b = new Bucket(K);
            b.addNode(new Node(from));
        } else {
            //TODO
        }

    }

    private void UponFindNodeReplyMessage(FindNodeReplyMessage msg, Host from, short sourceProto, int channelId) {
        logger.info("Received {} from {}", msg, from);
        //TODO
    }

    protected void uponMessageFail(ProtoMessage msg, Host host, short destProto, Throwable throwable, int channelId) {
        logger.error("Message {} to {} failed, reason: {}", msg, host, throwable);
    }

    /**
     *  Gets distance between two nodes
     * @param id1 - id of node1
     * @param id2 - id of node2
     */
    protected BigInteger getDistance(BigInteger id1, BigInteger id2) {
        return id1.xor(id2);
    }
}

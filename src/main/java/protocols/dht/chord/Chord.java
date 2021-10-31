package protocols.dht.chord;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.dht.messages.FindSuccessorMessage;
import protocols.dht.messages.FindSuccessorReplyMessage;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.channel.tcp.TCPChannel;
import pt.unl.fct.di.novasys.channel.tcp.events.*;
import pt.unl.fct.di.novasys.network.data.Host;
import utils.HashGenerator;

import java.io.IOException;
import java.math.BigInteger;
import java.net.InetAddress;
import java.util.*;

public class Chord extends GenericProtocol {

    private static final Logger logger = LogManager.getLogger(Chord.class);

    public static final int SHA1_HASH_SIZE = 160;

    // Protocol information, to register in babel
    public static final String PROTOCOL_NAME = "Chord";
    public static final short PROTOCOL_ID = 201;

    // State of the node
    private Host[] finger;
    private int fingerCount;
    private Host predecessor;

    private Host myself;
    private BigInteger myKey;

    private final Set<Host> pendingConnections; // Connections I'm trying to establish
    private final Map<Host, List<ProtoMessage>> pendingMessages; // Messages waiting for a outConnection to be opened to send a findSuccessor msg

    private final int channelId; //Id of the created channel


    public Chord(Properties props, Host myself) throws IOException, HandlerRegistrationException {
        super(PROTOCOL_NAME, PROTOCOL_ID);
        this.finger = new Host[SHA1_HASH_SIZE];
        this.fingerCount = 0;
        this.predecessor = null;
        this.myself = myself;
        this.myKey = HashGenerator.generateHash(myself.toString());
        this.pendingConnections = new HashSet<>();
        this.pendingMessages = new HashMap<>();


        String cMetricsInterval = props.getProperty("channel_metrics_interval", "10000"); //10 seconds
        //Create a properties object to setup channel-specific properties. See the channel description for more details.
        Properties channelProps = new Properties();
        channelProps.setProperty(TCPChannel.ADDRESS_KEY, props.getProperty("address")); //The address to bind to
        channelProps.setProperty(TCPChannel.PORT_KEY, props.getProperty("port")); //The port to bind to
        channelProps.setProperty(TCPChannel.METRICS_INTERVAL_KEY, cMetricsInterval); //The interval to receive channel metrics
        channelProps.setProperty(TCPChannel.HEARTBEAT_INTERVAL_KEY, "1000"); //Heartbeats interval for established connections
        channelProps.setProperty(TCPChannel.HEARTBEAT_TOLERANCE_KEY, "3000"); //Time passed without heartbeats until closing a connection
        channelProps.setProperty(TCPChannel.CONNECT_TIMEOUT_KEY, "1000"); //TCP connect timeout
        channelId = createChannel(TCPChannel.NAME, channelProps); //Create the channel with the given properties

        /*-------------------- Register Channel Events ------------------------------- */
        registerChannelEventHandler(channelId, OutConnectionDown.EVENT_ID, this::uponOutConnectionDown);
        registerChannelEventHandler(channelId, OutConnectionFailed.EVENT_ID, this::uponOutConnectionFailed);
        registerChannelEventHandler(channelId, OutConnectionUp.EVENT_ID, this::uponOutConnectionUp);
        registerChannelEventHandler(channelId, InConnectionUp.EVENT_ID, this::uponInConnectionUp);
        registerChannelEventHandler(channelId, InConnectionDown.EVENT_ID, this::uponInConnectionDown);

        /*---------------------- Register Message Serializers ---------------------- */
        registerMessageSerializer(channelId, FindSuccessorMessage.MSG_ID, FindSuccessorMessage.serializer);

        /*---------------------- Register Message Handlers -------------------------- */
        registerMessageHandler(channelId, FindSuccessorMessage.MSG_ID, this::UponFindSuccessorMessage, this::uponMsgFail);
    }

    @Override
    public void init(Properties props) throws HandlerRegistrationException, IOException {

        if(props.containsKey("contact")){   // Join ring
            String contact = props.getProperty("contact");
            String[] hostElems = contact.split(":");
            Host contactHost = new Host(InetAddress.getByName(hostElems[0]), Short.parseShort(hostElems[1]));
            predecessor = null;
            pendingConnections.add(contactHost);
            openConnection(contactHost);
            FindSuccessorMessage msg = new FindSuccessorMessage(myKey, myself);
            Objects.requireNonNull(pendingMessages.put(contactHost, new LinkedList<>())).add(msg);
        }
        else {  // create ring
            predecessor = null;
            finger[0] = myself;
        }
    }


    /* --------------------------------- TCPChannel Events ---------------------------- */

    private void uponOutConnectionUp(OutConnectionUp event, int channelId) {
        Host peer = event.getNode();
        logger.debug("Connection to {} is up", peer);
        pendingConnections.remove(peer);

        List<ProtoMessage> messages = pendingMessages.computeIfAbsent(peer, k -> new LinkedList<>());
        for (ProtoMessage m : messages)
            sendMessage(m, peer);

    }


    private void uponOutConnectionDown(OutConnectionDown event, int channelId) {
        //Host peer = event.getNode();
        //logger.debug("Connection to {} is down cause {}", peer, event.getCause());
        //membership.remove(event.getNode());
        //triggerNotification(new NeighbourDown(event.getNode()));
    }


    private void uponOutConnectionFailed(OutConnectionFailed<ProtoMessage> event, int channelId) {
        //logger.debug("Connection to {} failed cause: {}", event.getNode(), event.getCause());
        //pending.remove(event.getNode());
    }


    private void uponInConnectionUp(InConnectionUp event, int channelId) {
        //logger.trace("Connection from {} is up", event.getNode());
    }


    private void uponInConnectionDown(InConnectionDown event, int channelId) {
        //logger.trace("Connection from {} is down, cause: {}", event.getNode(), event.getCause());
    }

    /*--------------------------------- Messages ---------------------------------------- */

    private void UponFindSuccessorMessage(FindSuccessorMessage msg, Host from, short sourceProto, int channelId) {
        logger.debug("Received {} from {}", msg, from);
        BigInteger successorKey = HashGenerator.generateHash(finger[0].toString());
        if( msg.getKey().compareTo(myKey) > 0 && msg.getKey().compareTo(successorKey) <= 0 ){
            sendMessage(new FindSuccessorReplyMessage(finger[0], msg.getKey()), msg.getHost());
        }
        else{
            Host closestPrecedingNode = closestPrecedingNode(msg.getKey());
            sendMessage(new FindSuccessorMessage(msg.getKey(), msg.getHost()), closestPrecedingNode);
        }
    }


    private void uponMsgFail(ProtoMessage msg, Host host, short destProto,
                             Throwable throwable, int channelId) {
        //If a message fails to be sent, for whatever reason, log the message and the reason
        logger.error("Message {} to {} failed, reason: {}", msg, host, throwable);
    }


    /*--------------------------------- aux ---------------------------------------- */

    private Host closestPrecedingNode(BigInteger id){
        BigInteger fingerKey;
        for(int i = fingerCount - 1; i > 0; i--){
            fingerKey = HashGenerator.generateHash(finger[i].toString());
            if( fingerKey.compareTo(myKey) > 0 && fingerKey.compareTo(id) <= 0 ){
                return finger[i];
            }
        }
        return myself;
    }

}

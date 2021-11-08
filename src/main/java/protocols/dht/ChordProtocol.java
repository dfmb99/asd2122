package protocols.dht;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.channel.tcp.TCPChannel;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.math.BigInteger;
import java.util.*;

public class ChordProtocol extends GenericProtocol {

    private static final Logger logger = LogManager.getLogger(ChordProtocol.class);

    //Protocol information, to register in babel
    public final static short PROTOCOL_ID = 201;
    public final static String PROTOCOL_NAME = "ChordProtocol";
    private final static short SHA1_HASH_SIZE = 160; // number of bits in the hash key (SHA-1)

    private final Host self;     //My own address/port
    private final Host[] finger; // Finger table containing up to m entries.
    private final Host predecessor; //The previous node on the identifier circle

    private final Random rnd;

    private final int channelId; //Id of the created channel

    public ChordProtocol(Properties props, Host self) throws IOException, HandlerRegistrationException {
        super(PROTOCOL_NAME, PROTOCOL_ID);

        this.self = self;
        this.finger = new Host[SHA1_HASH_SIZE];
        this.predecessor = null;


        this.rnd = new Random();

        //Get some configurations from the Properties object
        //this.subsetSize = Integer.parseInt(props.getProperty("sample_size", "6"));
        //this.sampleTime = Integer.parseInt(props.getProperty("sample_time", "2000")); //2 seconds

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

        /*---------------------- Register Message Serializers ---------------------- */
        // registerMessageSerializer(channelId, SampleMessage.MSG_ID, SampleMessage.serializer);

        /*---------------------- Register Message Handlers -------------------------- */
        // registerMessageHandler(channelId, SampleMessage.MSG_ID, this::uponSample, this::uponMsgFail);

        /*--------------------- Register Timer Handlers ----------------------------- */
        // registerTimerHandler(SampleTimer.TIMER_ID, this::uponSampleTimer);
        // registerTimerHandler(InfoTimer.TIMER_ID, this::uponInfoTime);

        /*-------------------- Register Channel Events ------------------------------- */
        // registerChannelEventHandler(channelId, OutConnectionDown.EVENT_ID, this::uponOutConnectionDown);
        // registerChannelEventHandler(channelId, OutConnectionFailed.EVENT_ID, this::uponOutConnectionFailed);
        // registerChannelEventHandler(channelId, OutConnectionUp.EVENT_ID, this::uponOutConnectionUp);
        // registerChannelEventHandler(channelId, InConnectionUp.EVENT_ID, this::uponInConnectionUp);
        // registerChannelEventHandler(channelId, InConnectionDown.EVENT_ID, this::uponInConnectionDown);
        // registerChannelEventHandler(channelId, ChannelMetrics.EVENT_ID, this::uponChannelMetrics);
    }

    @Override
    public void init(Properties properties) throws HandlerRegistrationException, IOException {

    }
}

import java.net.InetAddress;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import protocols.apps.AutomatedApplication;
import protocols.dht.chord.ChordProtocol;
import protocols.dht.kademlia.KademliaProtocol;
import protocols.storage.StorageProtocol;
import pt.unl.fct.di.novasys.babel.core.Babel;
import pt.unl.fct.di.novasys.network.data.Host;
import utils.InterfaceToIp;


public class Main {

    //Sets the log4j (logging library) configuration file
    static {
        System.setProperty("log4j.configurationFile", "log4j2.xml");
    }

    //Creates the logger object
    private static final Logger logger = LogManager.getLogger(Main.class);

    //Default babel configuration file (can be overridden by the "-config" launch argument)
    private static final String DEFAULT_CONF = "babel_config.properties";

    public static void main(String[] args) throws Exception {


        //Get the (singleton) babel instance
        Babel babel = Babel.getInstance();

        //Loads properties from the configuration file, and merges them with properties passed in the launch arguments
        Properties props = Babel.loadConfig(args, DEFAULT_CONF);

        //If you pass an interface name in the properties (either file or arguments), this wil get the IP of that interface
        //and create a property "address=ip" to be used later by the channels.
        InterfaceToIp.addInterfaceIp(props);

        //The Host object is an address/port pair that represents a network host. It is used extensively in babel
        //It implements equals and hashCode, and also includes a serializer that makes it easy to use in network messages
        Host myself =  new Host(InetAddress.getByName(props.getProperty("address")),
                Integer.parseInt(props.getProperty("port")));

        // Application
        AutomatedApplication app = new AutomatedApplication(myself, props, StorageProtocol.PROTOCOL_ID);
        // Storage Protocol
        StorageProtocol storage = new StorageProtocol(props, myself, ChordProtocol.PROTOCOL_ID);
        // DHT Protocol
        ChordProtocol dht = new ChordProtocol(props, myself);
        //KademliaProtocol dht = new KademliaProtocol(props, myself);

        //Register applications in babel
        babel.registerProtocol(app);
        babel.registerProtocol(storage);
        babel.registerProtocol(dht);

        //Init the protocols. This should be done after creating all protocols, since there can be inter-protocol
        //communications in this step.
        app.init(props);
        storage.init(props);
        dht.init(props);

        //Start babel and protocol threads
        babel.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> logger.debug("Goodbye")));
    }

}

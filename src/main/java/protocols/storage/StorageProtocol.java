package protocols.storage;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.dht.replies.LookupReply;
import protocols.dht.requests.LookupRequest;
import protocols.storage.requests.RetrieveRequest;
import protocols.storage.requests.StoreRequest;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Properties;

public class StorageProtocol extends GenericProtocol {

    private static final Logger logger = LogManager.getLogger(StorageProtocol.class);

    //Protocol information, to register in babel
    public static final String PROTOCOL_NAME = "StorageProtocol";
    public static final short PROTOCOL_ID = 200;

    private final short dhtProtoId;

    private final Host myself; //My own address/port

    public StorageProtocol(Host host, short dhtProtoId){
        super(PROTOCOL_NAME, PROTOCOL_ID);
        this.dhtProtoId = dhtProtoId;
        this.myself = host;
    }

    @Override
    public void init(Properties props) throws HandlerRegistrationException {

        /*--------------------- Register Request Handlers ----------------------------- */
        registerRequestHandler(StoreRequest.REQUEST_ID, this::uponStoreRequest);
        registerRequestHandler(StoreRequest.REQUEST_ID, this::uponRetrieveRequest);

        /*--------------------- Register Reply Handlers ----------------------------- */
        registerReplyHandler(LookupReply.REPLY_ID, this::uponLookupReply);
    }

    private void uponStoreRequest(StoreRequest request, short sourceProto) {

        // TODO: do something

    }

    private void uponRetrieveRequest(RetrieveRequest request, short sourceProto) {

        LookupRequest lookupRequest = new LookupRequest(BigInteger.valueOf(request.getId())); // o id deve ser o do request?
        sendRequest(lookupRequest, dhtProtoId);

    }

    private void uponLookupReply(LookupReply reply, short sourceProto){

        // TODO: do something
    }

}

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
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class StorageProtocol extends GenericProtocol {

    private static final Logger logger = LogManager.getLogger(StorageProtocol.class);

    //Protocol information, to register in babel
    public static final String PROTOCOL_NAME = "StorageProtocol";
    public static final short PROTOCOL_ID = 200;

    //
    private static final short STORE_REQUEST = 0;
    private static final short RETRIEVE_REQUEST = 1;

    private final short dhtProtoId;

    private final Host myself; //My own address/port

    private Map<Short, Short> lookupReqs;    // (k, v) = (id of the request, type of request)

    public StorageProtocol(Host host, short dhtProtoId){
        super(PROTOCOL_NAME, PROTOCOL_ID);
        this.dhtProtoId = dhtProtoId;
        this.myself = host;
        this.lookupReqs = new HashMap<>();
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

        LookupRequest lookupRequest = new LookupRequest(BigInteger.valueOf(request.getId()));
        lookupReqs.put(request.getId(), STORE_REQUEST);
        sendRequest(lookupRequest, dhtProtoId);

    }

    private void uponRetrieveRequest(RetrieveRequest request, short sourceProto) {

        LookupRequest lookupRequest = new LookupRequest(BigInteger.valueOf(request.getId()));
        lookupReqs.put(request.getId(), RETRIEVE_REQUEST);
        sendRequest(lookupRequest, dhtProtoId);

    }

    private void uponLookupReply(LookupReply reply, short sourceProto){

        switch(lookupReqs.get(reply.getId())){
            case STORE_REQUEST:
                // TODO
                break;
            case RETRIEVE_REQUEST:
                // TODO
                break;
        }
    }

}

package protocols.storage;

import channel.notifications.ChannelCreated;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.dht.notifications.LookupResult;
import protocols.dht.requests.LookupRequest;
import protocols.storage.requests.RetrieveRequest;
import protocols.storage.requests.StoreRequest;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.generic.ProtoNotification;
import pt.unl.fct.di.novasys.network.data.Host;
import utils.HashGenerator;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

public class StorageProtocol extends GenericProtocol {

    private static final Logger logger = LogManager.getLogger(StorageProtocol.class);

    //Protocol information, to register in babel
    public static final String PROTOCOL_NAME = "StorageProtocol";
    public static final short PROTOCOL_ID = 200;

    enum RequestType {
        STORE_REQUEST,
        RETRIEVE_REQUEST
    }

    private final short dhtProtoId;

    private final Host myself;

    private Map<UUID, RequestType> pendingRequests;

    public StorageProtocol(Host host, short dhtProtoId){
        super(PROTOCOL_NAME, PROTOCOL_ID);
        this.dhtProtoId = dhtProtoId;
        this.myself = host;
        this.pendingRequests = new HashMap<>();
    }

    @Override
    public void init(Properties props) throws HandlerRegistrationException {

        /*--------------------- Register Request Handlers ----------------------------- */
        registerRequestHandler(StoreRequest.REQUEST_ID, this::uponStoreRequest);
        registerRequestHandler(StoreRequest.REQUEST_ID, this::uponRetrieveRequest);

        /*--------------------- Register Notification Handlers ----------------------------- */
        //subscribeNotification(ChannelCreated.NOTIFICATION_ID, this::uponChannelCreated);
        subscribeNotification(LookupResult.NOTIFICATION_ID, this::uponLookupResult);
    }

    private void uponStoreRequest(StoreRequest request, short sourceProto) {
        LookupRequest lookupRequest = new LookupRequest(
                HashGenerator.generateHash(request.getName())
        );
        sendRequest(lookupRequest, dhtProtoId);
        pendingRequests.put(request.getRequestId(), RequestType.STORE_REQUEST);
    }

    private void uponRetrieveRequest(RetrieveRequest request, short sourceProto) {
        LookupRequest lookupRequest = new LookupRequest(
                HashGenerator.generateHash(request.getName())
        );
        sendRequest(lookupRequest, dhtProtoId);
        pendingRequests.put(request.getRequestId(), RequestType.RETRIEVE_REQUEST);
    }

    private void uponLookupResult(LookupResult notification, short sourceProto) {
        switch(pendingRequests.remove(notification.getRequestId())){
            case STORE_REQUEST:
                // TODO
                break;
            case RETRIEVE_REQUEST:
                // TODO
                break;
        }
    }

}

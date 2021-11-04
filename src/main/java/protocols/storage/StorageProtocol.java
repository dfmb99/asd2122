package protocols.storage;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.BaseProtocol;
import protocols.dht.notifications.LookupResult;
import protocols.dht.requests.LookupRequest;
import protocols.storage.requests.RetrieveRequest;
import protocols.storage.requests.StoreRequest;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.channel.tcp.events.*;
import pt.unl.fct.di.novasys.network.data.Host;
import utils.HashGenerator;

import java.io.IOException;
import java.util.*;

public class StorageProtocol extends BaseProtocol {

    private static final Logger logger = LogManager.getLogger(StorageProtocol.class);

    public static final String PROTOCOL_NAME = "StorageProtocol";
    public static final short PROTOCOL_ID = 200;

    private final short dhtProtoId;

    enum RequestType {
        STORE_REQUEST,
        RETRIEVE_REQUEST
    }

    private Map<UUID, RequestType> pendingRequests;

    public StorageProtocol(Properties props, Host self, short dhtProtoId) throws IOException, HandlerRegistrationException {
        super(props, self, PROTOCOL_NAME, PROTOCOL_ID, logger);
        this.dhtProtoId = dhtProtoId;

        this.pendingRequests = new HashMap<>();

        /*--------------------- Register Request Handlers ----------------------------- */
        registerRequestHandler(StoreRequest.REQUEST_ID, this::uponStoreRequest);
        registerRequestHandler(StoreRequest.REQUEST_ID, this::uponRetrieveRequest);

        /*--------------------- Register Notification Handlers ----------------------------- */
        //subscribeNotification(ChannelCreated.NOTIFICATION_ID, this::uponChannelCreated);
        subscribeNotification(LookupResult.NOTIFICATION_ID, this::uponLookupResult);


    }

    /*--------------------------------- Initialization -------------------------------------- */

    @Override
    public void init(Properties props) {}

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

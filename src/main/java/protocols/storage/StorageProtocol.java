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
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.channel.tcp.events.*;
import pt.unl.fct.di.novasys.network.data.Host;
import utils.HashGenerator;

import java.util.*;

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

    /*---------------------------------- Connections ---------------------------------- */

    private final Set<Host> openConnections = new HashSet<>();
    private final Set<Host> pendingConnections = new HashSet<>();
    private final Map<Host, List<ProtoMessage>> pendingMessages = new HashMap<>();

    private void mendConnection(Host peer) {
        if(!openConnections.contains(peer) && !pendingConnections.contains(peer)) {
            openConnection(peer);
            pendingConnections.add(peer);
        }
    }

    private void dispatchMessage(ProtoMessage message, Host peer) {
        if(openConnections.contains(peer)) {
            sendMessage(message, peer);
        }
        else if(pendingConnections.contains(peer)) {
            pendingMessages.get(peer).add(message);
        }
        else {
            openConnection(peer);
            pendingConnections.add(peer);
            pendingMessages.put(peer, new LinkedList<>(Collections.singleton(message)));
        }
    }

    private void uponOutConnectionUp(OutConnectionUp event, int channelId) {
        Host peer = event.getNode();
        logger.info("Connection to {} is up", peer);
        openConnections.add(peer);
        pendingConnections.remove(peer);

        for (ProtoMessage m : pendingMessages.get(peer))
            sendMessage(m, peer);

        pendingMessages.remove(peer);
    }

    private void uponOutConnectionDown(OutConnectionDown event, int channelId) {
        Host peer = event.getNode();
        logger.info("Connection to {} is down cause {}", peer, event.getCause());
        openConnections.remove(peer);
        pendingConnections.remove(peer);
        pendingMessages.remove(peer);
    }


    private void uponOutConnectionFailed(OutConnectionFailed<ProtoMessage> event, int channelId) {
        Host peer = event.getNode();
        logger.info("Connection to {} failed cause: {}", event.getNode(), event.getCause());
        openConnections.remove(peer);
        pendingConnections.remove(peer);
        pendingMessages.remove(peer);
    }


    private void uponInConnectionUp(InConnectionUp event, int channelId) {
        Host peer = event.getNode();
        logger.trace("Connection from {} is up", peer);
        openConnections.add(peer);
        pendingConnections.remove(peer);

        for (ProtoMessage m : pendingMessages.get(peer))
            sendMessage(m, peer);

        pendingMessages.remove(peer);
    }


    private void uponInConnectionDown(InConnectionDown event, int channelId) {
        Host peer = event.getNode();
        logger.trace("Connection from {} is down, cause: {}", peer, event.getCause());
        openConnections.remove(peer);
        pendingMessages.remove(peer);
    }

    private void uponMessageFail(ProtoMessage msg, Host host, short destProto, Throwable throwable, int channelId) {
        logger.error("Message {} to {} failed, reason: {}", msg, host, throwable);
    }

    /*--------------------------------- Initialization -------------------------------------- */

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

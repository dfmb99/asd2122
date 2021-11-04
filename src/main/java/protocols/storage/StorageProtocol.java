package protocols.storage;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.BaseProtocol;
import protocols.dht.notifications.ChannelCreated;
import protocols.dht.replies.LookupReply;
import protocols.dht.requests.LookupRequest;
import protocols.storage.messages.RetrieveContentMessage;
import protocols.storage.messages.RetrieveContentReplyMessage;
import protocols.storage.messages.StoreContentMessage;
import protocols.storage.replies.RetrieveOKReply;
import protocols.storage.requests.RetrieveRequest;
import protocols.storage.requests.StoreRequest;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.babel.generic.ProtoRequest;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.util.*;

public class StorageProtocol extends BaseProtocol {

    private static final Logger logger = LogManager.getLogger(StorageProtocol.class);

    public static final String PROTOCOL_NAME = "StorageProtocol";
    public static final short PROTOCOL_ID = 200;

    private final short dhtProtoId;

    private Map<UUID, ProtoRequest> pendingRequests;

    public boolean ready;

    public StorageProtocol(Properties props, Host self, short dhtProtoId) throws IOException, HandlerRegistrationException {
        super(props, self, PROTOCOL_NAME, PROTOCOL_ID, logger, false);
        this.dhtProtoId = dhtProtoId;

        this.pendingRequests = new HashMap<>();

        /*--------------------- Register Request Handlers ----------------------------- */
        registerRequestHandler(StoreRequest.REQUEST_ID, this::uponStoreRequest);
        registerRequestHandler(RetrieveRequest.REQUEST_ID, this::uponRetrieveRequest);

        /*--------------------- Register Reply Handlers ----------------------------- */
        registerReplyHandler(LookupReply.REPLY_ID, this::uponLookupReply);

        /*--------------------- Register Notification Handlers ----------------------------- */
        subscribeNotification(ChannelCreated.NOTIFICATION_ID, this::uponChannelCreated);

        this.ready = false;
    }

    private void uponChannelCreated(ChannelCreated notification, short sourceProto) {
        this.channelId = notification.getChannelId();

        /*---------------------- Register Message Handlers -------------------------- */
        try {
            registerMessageHandler(channelId, StoreContentMessage.MSG_ID, this::uponStoreContentMessage, this::uponMessageFail);
            registerMessageHandler(channelId, RetrieveContentMessage.MSG_ID, this::uponRetrieveContentMessage, this::uponMessageFail);
            registerMessageHandler(channelId, RetrieveContentReplyMessage.MSG_ID, this::uponRetrieveContentReplyMessage, this::uponMessageFail);
        } catch (HandlerRegistrationException e) {
            logger.error("Error registering message handler: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }

        /*---------------------- Register Message Serializers ---------------------- */
        registerMessageSerializer(channelId, StoreContentMessage.MSG_ID, StoreContentMessage.serializer);
        registerMessageSerializer(channelId, RetrieveContentMessage.MSG_ID, RetrieveContentMessage.serializer);
        registerMessageSerializer(channelId, RetrieveContentReplyMessage.MSG_ID, RetrieveContentReplyMessage.serializer);

        ready = true;
    }

    @Override
    public void init(Properties props) {}

    /*----------------------------------- Requests Handler---------------------------------- */

    private void uponStoreRequest(StoreRequest request, short sourceProto) {
        LookupRequest lookupRequest = new LookupRequest(
                request.getName(), request.getRequestId()
        );
        sendRequest(lookupRequest, dhtProtoId);
        pendingRequests.put(request.getRequestId(), request);
    }

    private void uponRetrieveRequest(RetrieveRequest request, short sourceProto) {
        LookupRequest lookupRequest = new LookupRequest(
                request.getName(), request.getRequestId()
        );
        sendRequest(lookupRequest, dhtProtoId);
        pendingRequests.put(request.getRequestId(), request);
    }

    /*----------------------------------- Replies Handler ---------------------------------- */

    private void uponLookupReply(LookupReply result, short sourceProto) {
        ProtoRequest request = pendingRequests.remove(result.getRequestId());

        if(request instanceof StoreRequest) {
            StoreRequest storeRequest = (StoreRequest) request;
            dispatchMessage(new StoreContentMessage(storeRequest.getName(), storeRequest.getContent()), result.getNode().getHost());
        }
        else if(request instanceof RetrieveRequest) {
            RetrieveRequest retrieveRequest = (RetrieveRequest) request;
            dispatchMessage(new RetrieveContentMessage(retrieveRequest.getName()), result.getNode().getHost());
        }
    }

    /*---------------------------------------- Messages ---------------------------------- */

    private void uponStoreContentMessage(StoreContentMessage msg, Host from, short sourceProto, int channelId) {
        logger.info("Received {} from {}", msg, from);
    }

    private void uponRetrieveContentMessage(RetrieveContentMessage msg, Host from, short sourceProto, int channelId) {
        logger.info("Received {} from {}", msg, from);
    }

    private void uponRetrieveContentReplyMessage(RetrieveContentReplyMessage msg, Host from, short sourceProto, int channelId) {
        logger.info("Received {} from {}", msg, from);
        if(msg.getContent() != null)
            sendReply(new RetrieveOKReply(), );
    }

    /*---------------------------------------- Debug ---------------------------------- */

    protected void uponMessageFail(ProtoMessage msg, Host host, short destProto, Throwable throwable, int channelId) {
        logger.error("Message {} to {} failed, reason: {}", msg, host, throwable);
    }

}

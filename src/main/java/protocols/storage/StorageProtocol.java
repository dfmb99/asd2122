package protocols.storage;

import notifications.ChannelCreated;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.BaseProtocol;
import protocols.apps.AutomatedApplication;
import protocols.dht.replies.LookupReply;
import protocols.dht.requests.LookupRequest;
import protocols.storage.messages.RetrieveContentMessage;
import protocols.storage.messages.RetrieveContentReplyMessage;
import protocols.storage.messages.StoreContentMessage;
import protocols.storage.messages.StoreContentReplyMessage;
import protocols.storage.replies.RetrieveFailedReply;
import protocols.storage.replies.RetrieveOKReply;
import protocols.storage.replies.StoreOKReply;
import protocols.storage.requests.RetrieveRequest;
import protocols.storage.requests.StoreRequest;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.babel.generic.ProtoRequest;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class StorageProtocol extends BaseProtocol {

    private static final Logger logger = LogManager.getLogger(StorageProtocol.class);

    public static final String PROTOCOL_NAME = "StorageProtocol";
    public static final short PROTOCOL_ID = 40;

    private final short dhtProtoId;

    private final Map<UUID, ProtoRequest> pendingRequests;

    private final IPersistentStorage storage;

    public boolean ready;

    public StorageProtocol(Properties props, Host self, short dhtProtoId) throws IOException, HandlerRegistrationException {
        super(props, self, PROTOCOL_NAME, PROTOCOL_ID, logger, false);
        this.dhtProtoId = dhtProtoId;

        this.pendingRequests = new ConcurrentHashMap<>();

        this.storage = new VolatileStorage();

        /*--------------------- Register Request Handlers ----------------------------- */
        registerRequestHandler(StoreRequest.REQUEST_TYPE_ID, this::uponStoreRequest);
        registerRequestHandler(RetrieveRequest.REQUEST_TYPE_ID, this::uponRetrieveRequest);

        /*--------------------- Register Reply Handlers ----------------------------- */
        registerReplyHandler(LookupReply.REPLY_TYPE_ID, this::uponLookupReply);
        
        /*--------------------- Register Notification Handlers ----------------------------- */
        subscribeNotification(ChannelCreated.NOTIFICATION_ID, this::uponChannelCreated);

        this.ready = false;
    }

    private void uponChannelCreated(ChannelCreated notification, short sourceProto) {
        this.channelId = notification.getChannelId();

        try {
            /*---------------------- Register Channel Events ------------------------------ */
            registerChannelEvents();
            /*---------------------- Register Message Handlers -------------------------- */
            registerMessageHandler(channelId, RetrieveContentMessage.MSG_ID, this::uponRetrieveContentMessage, this::uponMessageFail);
            registerMessageHandler(channelId, RetrieveContentReplyMessage.MSG_ID, this::uponRetrieveContentReplyMessage, this::uponMessageFail);
            registerMessageHandler(channelId, StoreContentMessage.MSG_ID, this::uponStoreContentMessage, this::uponMessageFail);
            registerMessageHandler(channelId, StoreContentReplyMessage.MSG_ID, this::uponStoreContentReplyMessage, this::uponMessageFail);
        } catch (Exception e) {
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
                request.getRequestId(), request.getName()
        );
        sendRequest(lookupRequest, dhtProtoId);
        pendingRequests.put(request.getRequestId(), request);
    }

    private void uponRetrieveRequest(RetrieveRequest request, short sourceProto) {
        LookupRequest lookupRequest = new LookupRequest(
                request.getRequestId(), request.getName()
        );
        sendRequest(lookupRequest, dhtProtoId);
        pendingRequests.put(request.getRequestId(), request);
    }

    /*----------------------------------- Replies Handler ---------------------------------- */

    private void uponLookupReply(LookupReply result, short sourceProto) {
        ProtoRequest request = pendingRequests.remove(result.getRequestId());

        if(request instanceof StoreRequest) {
            StoreRequest storeRequest = (StoreRequest) request;
            if(result.getNode().getHost().equals(self)) {
                storage.put(storeRequest.getName(), storeRequest.getContent());
                sendReply(new StoreOKReply(storeRequest.getRequestId(), storeRequest.getName()), AutomatedApplication.PROTO_ID);
            }
            else dispatchMessage(new StoreContentMessage(result.getRequestId(), storeRequest.getName(), storeRequest.getContent()), result.getNode().getHost());
        }


        else if(request instanceof RetrieveRequest) {
            RetrieveRequest retrieveRequest = (RetrieveRequest) request;
            if(result.getNode().getHost().equals(self)) {
                byte[] content = storage.get(retrieveRequest.getName());
                if(content != null)
                    sendReply(new RetrieveOKReply(retrieveRequest.getRequestId(), retrieveRequest.getName(), content), AutomatedApplication.PROTO_ID);
                else
                    sendReply(new RetrieveFailedReply(retrieveRequest.getRequestId(), retrieveRequest.getName()), AutomatedApplication.PROTO_ID);
            }
            else dispatchMessage(new RetrieveContentMessage(result.getRequestId(), retrieveRequest.getName()), result.getNode().getHost());
        }
    }

    /*---------------------------------------- Messages ---------------------------------- */

    private void uponStoreContentMessage(StoreContentMessage msg, Host from, short sourceProto, int channelId) {
        logger.info("Received {} from {}", msg, from);
        storage.put(msg.getName(), msg.getContent());
        dispatchMessage(new StoreContentReplyMessage(msg.getRequestId(), msg.getName()), from);
    }

    private void uponStoreContentReplyMessage(StoreContentReplyMessage msg, Host from, short sourceProto, int channelId) {
        logger.info("Received {} from {}", msg, from);
        sendReply(new StoreOKReply(msg.getRequestId(), msg.getName()), AutomatedApplication.PROTO_ID);
    }

    private void uponRetrieveContentMessage(RetrieveContentMessage msg, Host from, short sourceProto, int channelId) {
        logger.info("Received {} from {}", msg, from);
        byte[] content = storage.get(msg.getName());
        dispatchMessage(new RetrieveContentReplyMessage(msg.getRequestId(), msg.getName(), content), from);
    }

    private void uponRetrieveContentReplyMessage(RetrieveContentReplyMessage msg, Host from, short sourceProto, int channelId) {
        logger.info("Received {} from {}", msg, from);
        if(msg.getContent() != null)
            sendReply(new RetrieveOKReply(msg.getRequestId(), msg.getName(), msg.getContent()), AutomatedApplication.PROTO_ID);
        else
            sendReply(new RetrieveFailedReply(msg.getRequestId(), msg.getName()), AutomatedApplication.PROTO_ID);
    }

    /*---------------------------------------- Debug ---------------------------------- */

    protected void uponMessageFail(ProtoMessage msg, Host host, short destProto, Throwable throwable, int channelId) {
        logger.error("Message {} to {} failed, reason: {}", msg, host, throwable);
    }

}

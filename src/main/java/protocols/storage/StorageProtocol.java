package protocols.storage;

import notifications.ChannelCreated;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.BaseProtocol;
import protocols.apps.AutomatedApplication;
import protocols.dht.replies.LookupReply;
import protocols.dht.requests.LookupRequest;
import protocols.dht.types.Node;
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
import pt.unl.fct.di.novasys.channel.tcp.events.*;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class StorageProtocol extends BaseProtocol {

    public static final Logger logger = LogManager.getLogger(StorageProtocol.class);

    public static final String PROTOCOL_NAME = "StorageProtocol";
    public static final short PROTOCOL_ID = 40;

    private final short dhtProtoId;

    private final Map<UUID, ProtoRequest> pendingRequests;

    private final IPersistentStorage storage;

    public boolean ready;

    public StorageProtocol(Properties props, Host self, short dhtProtoId) throws IOException, HandlerRegistrationException {
        super(self, PROTOCOL_NAME, PROTOCOL_ID, logger);

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
        try {
            setChannel(notification.channel);

            /*---------------------- Register Channel Events ------------------------------ */
            registerChannelEventHandler(channel.id, OutConnectionDown.EVENT_ID, this::uponOutConnectionDown);
            registerChannelEventHandler(channel.id, OutConnectionFailed.EVENT_ID, this::uponOutConnectionFailed);
            registerChannelEventHandler(channel.id, OutConnectionUp.EVENT_ID, this::uponOutConnectionUp);
            registerChannelEventHandler(channel.id, InConnectionUp.EVENT_ID, this::uponInConnectionUp);
            registerChannelEventHandler(channel.id, InConnectionDown.EVENT_ID, this::uponInConnectionDown);

            /*---------------------- Register Message Handlers -------------------------- */
            registerMessageHandler(channel.id, RetrieveContentMessage.MSG_ID, this::uponRetrieveContentMessage, this::uponMessageFail);
            registerMessageHandler(channel.id, RetrieveContentReplyMessage.MSG_ID, this::uponRetrieveContentReplyMessage, this::uponMessageFail);
            registerMessageHandler(channel.id, StoreContentMessage.MSG_ID, this::uponStoreContentMessage, this::uponMessageFail);
            registerMessageHandler(channel.id, StoreContentReplyMessage.MSG_ID, this::uponStoreContentReplyMessage, this::uponMessageFail);

            /*---------------------- Register Message Serializers ---------------------- */
            registerMessageSerializer(channel.id, RetrieveContentMessage.MSG_ID, RetrieveContentMessage.serializer);
            registerMessageSerializer(channel.id, RetrieveContentReplyMessage.MSG_ID, RetrieveContentReplyMessage.serializer);
            registerMessageSerializer(channel.id, StoreContentMessage.MSG_ID, StoreContentMessage.serializer);
            registerMessageSerializer(channel.id, StoreContentReplyMessage.MSG_ID, StoreContentReplyMessage.serializer);

            ready = true;
        } catch (Exception e) {
            logger.error("Error registering message handler: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    @Override
    public void init(Properties props) {}

    /*----------------------------------- Requests Handler---------------------------------- */

    private void uponStoreRequest(StoreRequest request, short sourceProto) {
        LookupRequest lookupRequest = new LookupRequest(
                request.getRequestId(), request.getName()
        );
        logger.info("Lookup request {}", request.getRequestId());
        sendRequest(lookupRequest, dhtProtoId);
        pendingRequests.put(request.getRequestId(), request);
    }

    private void uponRetrieveRequest(RetrieveRequest request, short sourceProto) {
        LookupRequest lookupRequest = new LookupRequest(
                request.getRequestId(), request.getName()
        );
        logger.info("Lookup request {}", request.getRequestId());
        sendRequest(lookupRequest, dhtProtoId);
        pendingRequests.put(request.getRequestId(), request);
    }

    /*----------------------------------- Replies Handler ---------------------------------- */

    private void uponLookupReply(LookupReply result, short sourceProto) {
        logger.info("Lookup reply {}", result.getRequestId());

        ProtoRequest request = pendingRequests.remove(result.getRequestId());

        if(request instanceof StoreRequest) {
            StoreRequest storeRequest = (StoreRequest) request;

            boolean selfIncluded = result.getNodes().stream().anyMatch(r -> r.getHost().equals(self));
            if(selfIncluded) {
                storage.put(storeRequest.getName(), storeRequest.getContent());
                logger.debug("Stored content {} in myself", storeRequest.getName());
                sendReply(new StoreOKReply(storeRequest.getRequestId(), storeRequest.getName()), AutomatedApplication.PROTO_ID);
            }

            for(Node node : result.getNodes())
                dispatchMessageButNotToSelf(new StoreContentMessage(result.getRequestId(), storeRequest.getName(), storeRequest.getContent()), node.getHost());
        }


        else if(request instanceof RetrieveRequest) {
            RetrieveRequest retrieveRequest = (RetrieveRequest) request;
            boolean selfIncluded = result.getNodes().stream().anyMatch(r -> r.getHost().equals(self));
            if(selfIncluded) {
                byte[] content = storage.get(retrieveRequest.getName());
                if(content != null) {
                    logger.debug("Failed to retrieved content {} from myself", retrieveRequest.getName());
                    sendReply(new RetrieveOKReply(retrieveRequest.getRequestId(), retrieveRequest.getName(), content), AutomatedApplication.PROTO_ID);
                }
                else {
                    logger.debug("Retrieved content {} from myself", retrieveRequest.getName());
                    sendReply(new RetrieveFailedReply(retrieveRequest.getRequestId(), retrieveRequest.getName()), AutomatedApplication.PROTO_ID);
                }
            }
            else {
                for(Node node : result.getNodes())
                    dispatchMessage(new RetrieveContentMessage(result.getRequestId(), retrieveRequest.getName()), node.getHost());
            }
        }
    }

    /*---------------------------------------- Messages ---------------------------------- */

    private void uponStoreContentMessage(StoreContentMessage msg, Host from, short sourceProto, int channelId) {
        logger.debug("Received {} from {}", msg, from);
        storage.put(msg.getName(), msg.getContent());
        dispatchMessage(new StoreContentReplyMessage(msg.getRequestId(), msg.getName()), from);
    }

    private void uponStoreContentReplyMessage(StoreContentReplyMessage msg, Host from, short sourceProto, int channelId) {
        logger.debug("Received {} from {}", msg, from);
        logger.debug("Stored content {} in {}", msg.getName(), from);
        sendReply(new StoreOKReply(msg.getRequestId(), msg.getName()), AutomatedApplication.PROTO_ID);
    }

    private void uponRetrieveContentMessage(RetrieveContentMessage msg, Host from, short sourceProto, int channelId) {
        logger.debug("Received {} from {}", msg, from);
        byte[] content = storage.get(msg.getName());
        dispatchMessage(new RetrieveContentReplyMessage(msg.getRequestId(), msg.getName(), content == null, content), from);
    }

    private void uponRetrieveContentReplyMessage(RetrieveContentReplyMessage msg, Host from, short sourceProto, int channelId) {
        logger.debug("Received {} from {}", msg, from);
        if(!msg.isNotFound()) {
            logger.debug("Retrieved content {} from {}", msg.getName(), from);
            sendReply(new RetrieveOKReply(msg.getRequestId(), msg.getName(), msg.getContent()), AutomatedApplication.PROTO_ID);
        }
        else {
            logger.debug("Failed to retrieved content {} from {}", msg.getName(), from);
            sendReply(new RetrieveFailedReply(msg.getRequestId(), msg.getName()), AutomatedApplication.PROTO_ID);
        }
    }

    /*---------------------------------------- Debug ---------------------------------- */

    protected void uponMessageFail(ProtoMessage msg, Host host, short destProto, Throwable throwable, int channelId) {
        logger.error("Message {} to {} failed, reason: {}", msg, host, throwable);
    }

}

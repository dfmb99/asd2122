package protocols.dht.replies;

import protocols.dht.chord.types.Node;
import pt.unl.fct.di.novasys.babel.generic.ProtoReply;

import java.util.UUID;

public class LookupReply extends ProtoReply {

    public static final short REPLY_ID = 116;

    private final UUID requestId;
    private final Node node;

    public LookupReply(UUID requestId, Node node) {
        super(REPLY_ID);
        this.requestId = requestId;
        this.node = node;
    }

    public UUID getRequestId() {
        return requestId;
    }

    public Node getNode() {
        return node;
    }
}

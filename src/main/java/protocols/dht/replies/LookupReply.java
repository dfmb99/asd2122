package protocols.dht.replies;

import protocols.dht.chord.types.ChordNode;
import pt.unl.fct.di.novasys.babel.generic.ProtoReply;

import java.util.UUID;

public class LookupReply extends ProtoReply {

    public static final short REPLY_TYPE_ID = 200;

    private final UUID requestId;
    private final ChordNode node;

    public LookupReply(UUID requestId, ChordNode node) {
        super(REPLY_TYPE_ID);
        this.requestId = requestId;
        this.node = node;
    }

    public UUID getRequestId() {
        return requestId;
    }

    public ChordNode getNode() {
        return node;
    }
}

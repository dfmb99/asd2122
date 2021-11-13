package protocols.dht.replies;

import protocols.dht.chord.types.ChordNode;
import pt.unl.fct.di.novasys.babel.generic.ProtoReply;

import java.util.UUID;

public class LookupReply extends ProtoReply {

    public static final short REPLY_TYPE_ID = 200;

    private final UUID requestId;
    private final ChordNode[] nodes;

    public LookupReply(UUID requestId, ChordNode[] nodes) {
        super(REPLY_TYPE_ID);
        this.requestId = requestId;
        this.nodes = nodes;
    }

    public UUID getRequestId() {
        return requestId;
    }

    public ChordNode[] getNodes() {
        return nodes;
    }
}
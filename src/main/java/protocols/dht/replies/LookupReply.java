package protocols.dht.replies;

import protocols.dht.chord.types.ChordNode;
import pt.unl.fct.di.novasys.babel.generic.ProtoReply;

import java.util.UUID;

public class LookupReply extends ProtoReply {

    public static final short REPLY_TYPE_ID = 200;

    private final UUID requestId;
    private final ChordNode node;
    private final List<KademliaNode> kad_nodes;

    public LookupReply(UUID requestId, ChordNode node) {
        super(REPLY_TYPE_ID);
        this.requestId = requestId;
        this.node = node;
        this.kad_nodes = null;
    }

    public LookupReply(UUID requestId, List<KademliaNode> kad_nodes) {
        super(REPLY_TYPE_ID);
        this.requestId = requestId;
        this.node = null;
        this.kad_nodes = kad_nodes;
    }

    public UUID getRequestId() {
        return requestId;
    }

    public ChordNode getNode() {
        return node;
    }
}

package protocols.dht.replies;

import protocols.dht.chord.types.ChordNode;
import protocols.dht.types.Node;
import pt.unl.fct.di.novasys.babel.generic.ProtoReply;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class LookupReply extends ProtoReply {

    public static final short REPLY_TYPE_ID = 200;

    private final UUID requestId;
    private final List<Node> nodes;

    public LookupReply(UUID requestId, Node node) {
        super(REPLY_TYPE_ID);
        this.requestId = requestId;
        this.nodes = new ArrayList<>();
        nodes.add(node);
    }

    public LookupReply(UUID requestId, List<Node> nodes) {
        super(REPLY_TYPE_ID);
        this.requestId = requestId;
        this.nodes = nodes;
    }

    public UUID getRequestId() {
        return requestId;
    }

    public List<Node> getNodes() {
        return nodes;
    }
}

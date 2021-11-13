package protocols.dht.replies;

import protocols.dht.types.Node;
import pt.unl.fct.di.novasys.babel.generic.ProtoReply;

import java.util.*;

public class LookupReply extends ProtoReply {

    public static final short REPLY_TYPE_ID = 200;

    private final UUID requestId;
    private final Set<Node> nodes;

    public LookupReply(UUID requestId, Node node) {
        super(REPLY_TYPE_ID);
        this.requestId = requestId;
        this.nodes = new HashSet<>();
        nodes.add(node);
    }

    public LookupReply(UUID requestId, Set<Node> nodes) {
        super(REPLY_TYPE_ID);
        this.requestId = requestId;
        this.nodes = nodes;
    }

    public UUID getRequestId() {
        return requestId;
    }

    public Set<Node> getNodes() {
        return nodes;
    }
}

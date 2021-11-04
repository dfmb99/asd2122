package protocols.dht.chord.messages;

import io.netty.buffer.ByteBuf;
import protocols.dht.chord.types.Node;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.io.IOException;

public class GetPredecessorReplyMessage extends ProtoMessage {

    public final static short MSG_ID = 104;

    private final Node predecessor;

    public GetPredecessorReplyMessage(Node predecessor) {
        super(MSG_ID);
        this.predecessor = predecessor;
    }

    public Node getPredecessor() {
        return predecessor;
    }

    @Override
    public String toString() {
        return "FindSuccessorReplyMessage{" +
                "predecessor=" + predecessor.toString() +
                '}';
    }

    public static ISerializer<GetPredecessorReplyMessage> serializer = new ISerializer<>() {
        public void serialize(GetPredecessorReplyMessage msg, ByteBuf out) throws IOException {
            Node.serializer.serialize(msg.getPredecessor(), out);
        }

        public GetPredecessorReplyMessage deserialize(ByteBuf in) throws IOException {
            Node node = Node.serializer.deserialize(in);
            return new GetPredecessorReplyMessage(node);
        }
    };
}




package protocols.dht.chord.messages;

import io.netty.buffer.ByteBuf;
import protocols.dht.chord.types.Node;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;

public class JoinRingReplyMessage extends ProtoMessage {

    public final static short MSG_ID = 104;

    private final Node successor;

    public JoinRingReplyMessage(Node successor) {
        super(MSG_ID);
        this.successor = successor;
    }

    public Node getSuccessor() {
        return successor;
    }

    @Override
    public String toString() {
        return "FindSuccessorReplyMessage{" +
                "successor=" + successor.toString() +
                '}';
    }

    public static ISerializer<JoinRingReplyMessage> serializer = new ISerializer<>() {
        public void serialize(JoinRingReplyMessage msg, ByteBuf out) throws IOException {
            Node.serializer.serialize(msg.getSuccessor(), out);
        }

        public JoinRingReplyMessage deserialize(ByteBuf in) throws IOException {
            Node node = Node.serializer.deserialize(in);
            return new JoinRingReplyMessage(node);
        }
    };
}




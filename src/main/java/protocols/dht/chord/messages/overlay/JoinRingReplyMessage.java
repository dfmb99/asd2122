package protocols.dht.chord.messages.overlay;

import io.netty.buffer.ByteBuf;
import protocols.dht.chord.types.Node;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.io.IOException;

public class JoinRingReplyMessage extends ProtoMessage {

    public final static short MSG_ID = 203;

    private final Node node;

    public JoinRingReplyMessage(Node node) {
        super(MSG_ID);
        this.node = node;
    }

    public Node getNode() {
        return node;
    }

    @Override
    public String toString() {
        return "FindSuccessorReplyMessage{" +
                "node=" + node.toString() +
                '}';
    }

    public static ISerializer<JoinRingReplyMessage> serializer = new ISerializer<>() {
        public void serialize(JoinRingReplyMessage sampleMessage, ByteBuf out) throws IOException {
            Node.serializer.serialize(sampleMessage.getNode(), out);
        }

        public JoinRingReplyMessage deserialize(ByteBuf in) throws IOException {
            Node node = Node.serializer.deserialize(in);
            return new JoinRingReplyMessage(node);
        }
    };
}




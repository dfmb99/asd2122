package protocols.dht.chord.messages.overlay;

import io.netty.buffer.ByteBuf;
import protocols.dht.chord.types.Node;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.io.IOException;

public class RestoreFingerReplyMessage extends ProtoMessage {

    public final static short MSG_ID = 207;

    private final int finger;
    private final Node node;

    public RestoreFingerReplyMessage(int finger, Node node) {
        super(MSG_ID);
        this.finger = finger;
        this.node = node;
    }

    public int getFinger() {
        return finger;
    }

    public Node getNode() {
        return node;
    }

    @Override
    public String toString() {
        return "RestoreFingerReplyMessage{" +
                "finger=" + finger +
                "node=" + node.toString() +
                '}';
    }

    public static ISerializer<RestoreFingerReplyMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(RestoreFingerReplyMessage sampleMessage, ByteBuf out) throws IOException {
            out.writeInt(sampleMessage.finger);
            Node.serializer.serialize(sampleMessage.node, out);
        }

        @Override
        public RestoreFingerReplyMessage deserialize(ByteBuf in) throws IOException {
            int finger = in.readInt();
            Node node = Node.serializer.deserialize(in);
            return new RestoreFingerReplyMessage(finger, node);
        }
    };
}




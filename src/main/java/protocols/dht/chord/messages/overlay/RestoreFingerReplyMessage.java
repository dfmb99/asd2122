package protocols.dht.chord.messages.overlay;

import io.netty.buffer.ByteBuf;
import protocols.dht.chord.types.ChordNode;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.io.IOException;

public class RestoreFingerReplyMessage extends ProtoMessage {

    public final static short MSG_ID = 207;

    private final int finger;
    private final ChordNode node;

    public RestoreFingerReplyMessage(int finger, ChordNode node) {
        super(MSG_ID);
        this.finger = finger;
        this.node = node;
    }

    public int getFinger() {
        return finger;
    }

    public ChordNode getNode() {
        return node;
    }

    @Override
    public String toString() {
        return "RestoreFingerReplyMessage{" +
                "finger=" + finger +
                ", node=" + node +
                '}';
    }

    public static ISerializer<RestoreFingerReplyMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(RestoreFingerReplyMessage sampleMessage, ByteBuf out) throws IOException {
            out.writeInt(sampleMessage.finger);
            ChordNode.serializer.serialize(sampleMessage.node, out);
        }

        @Override
        public RestoreFingerReplyMessage deserialize(ByteBuf in) throws IOException {
            int finger = in.readInt();
            ChordNode node = ChordNode.serializer.deserialize(in);
            return new RestoreFingerReplyMessage(finger, node);
        }
    };
}




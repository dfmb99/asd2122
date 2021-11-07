package protocols.dht.chord.messages.overlay;

import io.netty.buffer.ByteBuf;
import protocols.dht.chord.types.ChordNode;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.io.IOException;

public class JoinRingMessage extends ProtoMessage {

    public final static short MSG_ID = 202;

    ChordNode node;

    public JoinRingMessage(ChordNode node) {
        super(MSG_ID);
        this.node = node;
    }

    public ChordNode getNode() {
        return node;
    }

    @Override
    public String toString() {
        return "JoinRingMessage{" +
                "node=" + node.toString() +
                '}';
    }

    public static ISerializer<JoinRingMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(JoinRingMessage msg, ByteBuf out) throws IOException {
            ChordNode.serializer.serialize(msg.node, out);
        }

        @Override
        public JoinRingMessage deserialize(ByteBuf in) throws IOException {
            ChordNode node = ChordNode.serializer.deserialize(in);
            return new JoinRingMessage(node);
        }
    };
}




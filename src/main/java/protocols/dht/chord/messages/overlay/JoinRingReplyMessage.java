package protocols.dht.chord.messages.overlay;

import io.netty.buffer.ByteBuf;
import protocols.dht.chord.ChordProtocol;
import protocols.dht.chord.types.ChordNode;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.io.IOException;

public class JoinRingReplyMessage extends ProtoMessage {

    public final static short MSG_ID = 203;

    private final ChordNode node;

    public JoinRingReplyMessage(ChordNode node) {
        super(MSG_ID);
        this.node = node;
    }

    public ChordNode getNode() {
        return node;
    }

    @Override
    public String toString() {
        return "JoinRingReplyMessage{" +
                "node=" + node.toString() +
                '}';
    }

    public static ISerializer<JoinRingReplyMessage> serializer = new ISerializer<>() {
        public void serialize(JoinRingReplyMessage sampleMessage, ByteBuf out) throws IOException {
            ChordNode.serializer.serialize(sampleMessage.getNode(), out);
            ChordProtocol.logger.info("Message sent with size {}", out.readableBytes());
        }

        public JoinRingReplyMessage deserialize(ByteBuf in) throws IOException {
            ChordProtocol.logger.info("Message received with size {}", in.readableBytes());
            ChordNode node = ChordNode.serializer.deserialize(in);
            return new JoinRingReplyMessage(node);
        }
    };
}




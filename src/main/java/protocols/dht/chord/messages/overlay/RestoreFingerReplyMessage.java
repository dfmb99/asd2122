package protocols.dht.chord.messages.overlay;

import io.netty.buffer.ByteBuf;
import protocols.dht.chord.ChordProtocol;
import protocols.dht.chord.types.ChordNode;
import protocols.dht.chord.types.ChordSegment;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.io.IOException;
import java.util.logging.Logger;

public class RestoreFingerReplyMessage extends ProtoMessage {

    public final static short MSG_ID = 207;

    private final ChordSegment segment;
    private final ChordNode node;

    public RestoreFingerReplyMessage(ChordSegment segment, ChordNode node) {
        super(MSG_ID);
        this.segment = segment;
        this.node = node;
    }

    public ChordSegment getSegment() {
        return segment;
    }

    public ChordNode getNode() {
        return node;
    }

    @Override
    public String toString() {
        return "RestoreFingerReplyMessage{" +
                "segment=" + segment +
                ", node=" + node +
                '}';
    }

    public static ISerializer<RestoreFingerReplyMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(RestoreFingerReplyMessage sampleMessage, ByteBuf out) throws IOException {
            ChordSegment.serializer.serialize(sampleMessage.segment, out);
            ChordNode.serializer.serialize(sampleMessage.node, out);
            ChordProtocol.logger.info("Message sent with size {}", out.readableBytes());
        }

        @Override
        public RestoreFingerReplyMessage deserialize(ByteBuf in) throws IOException {
            ChordProtocol.logger.info("Message received with size {}", in.readableBytes());
            ChordSegment segment = ChordSegment.serializer.deserialize(in);
            ChordNode node = ChordNode.serializer.deserialize(in);
            return new RestoreFingerReplyMessage(segment, node);
        }
    };
}




package protocols.dht.chord.messages.overlay;

import io.netty.buffer.ByteBuf;
import protocols.dht.chord.types.ChordNode;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.io.IOException;

public class GetPredecessorReplyMessage extends ProtoMessage {

    public final static short MSG_ID = 201;

    private final ChordNode predecessor;

    public GetPredecessorReplyMessage(ChordNode predecessor) {
        super(MSG_ID);
        this.predecessor = predecessor;
    }

    public ChordNode getPredecessor() {
        return predecessor;
    }

    @Override
    public String toString() {
        return "GetPredecessorReplyMessage{" +
                "predecessor=" + predecessor.toString() +
                '}';
    }

    public static ISerializer<GetPredecessorReplyMessage> serializer = new ISerializer<>() {
        public void serialize(GetPredecessorReplyMessage sampleMessage, ByteBuf out) throws IOException {
            ChordNode.serializer.serialize(sampleMessage.getPredecessor(), out);
        }

        public GetPredecessorReplyMessage deserialize(ByteBuf in) throws IOException {
            ChordNode node = ChordNode.serializer.deserialize(in);
            return new GetPredecessorReplyMessage(node);
        }
    };
}




package protocols.dht.chord.messages.search;

import io.netty.buffer.ByteBuf;
import protocols.dht.chord.types.ChordKey;
import protocols.dht.chord.types.ChordNode;
import protocols.dht.types.Node;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.io.IOException;
import java.util.Arrays;
import java.util.UUID;

public class FindSuccessorReplyMessage extends ProtoMessage {

    public final static short MSG_ID = 209;

    private final UUID requestId;
    private final ChordKey key;
    private final ChordNode successor;

    public FindSuccessorReplyMessage(UUID requestId, ChordKey key, ChordNode successor) {
        super(MSG_ID);
        this.requestId = requestId;
        this.key = key;
        this.successor = successor;
    }

    public UUID getRequestId() {
        return requestId;
    }

    public ChordKey getKey() {
        return key;
    }

    public ChordNode getSuccessor() {
        return successor;
    }

    @Override
    public String toString() {
        return "FindSuccessorReplyMessage{" +
                "requestId=" + requestId +
                ", key=" + key +
                ", successor=" + successor +
                '}';
    }

    public static ISerializer<FindSuccessorReplyMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(FindSuccessorReplyMessage sampleMessage, ByteBuf out) throws IOException {
            out.writeLong(sampleMessage.requestId.getMostSignificantBits());
            out.writeLong(sampleMessage.requestId.getLeastSignificantBits());
            ChordKey.serializer.serialize(sampleMessage.key, out);
            ChordNode.serializer.serialize(sampleMessage.successor, out);
        }

        @Override
        public FindSuccessorReplyMessage deserialize(ByteBuf in) throws IOException {
            UUID requestId = new UUID(in.readLong(), in.readLong());
            ChordKey key = ChordKey.serializer.deserialize(in);
            ChordNode successor = ChordNode.serializer.deserialize(in);
            return new FindSuccessorReplyMessage(requestId, key, successor);
        }
    };
}




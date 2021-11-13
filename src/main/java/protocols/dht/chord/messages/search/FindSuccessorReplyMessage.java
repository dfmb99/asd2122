package protocols.dht.chord.messages.search;

import io.netty.buffer.ByteBuf;
import protocols.dht.chord.types.ChordKey;
import protocols.dht.chord.types.ChordNode;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.io.IOException;
import java.util.Arrays;
import java.util.UUID;

public class FindSuccessorReplyMessage extends ProtoMessage {

    public final static short MSG_ID = 209;

    private final UUID requestId;
    private final ChordKey key;
    private final ChordNode[] successors;

    public FindSuccessorReplyMessage(UUID requestId, ChordKey key, ChordNode[] successors) {
        super(MSG_ID);
        this.requestId = requestId;
        this.key = key;
        this.successors = successors;
    }

    public UUID getRequestId() {
        return requestId;
    }

    public ChordKey getKey() {
        return key;
    }

    public ChordNode[] getSuccessors() {
        return successors;
    }

    @Override
    public String toString() {
        return "FindSuccessorReplyMessage{" +
                "requestId=" + requestId +
                ", key=" + key +
                ", successors=" + Arrays.toString(successors) +
                '}';
    }

    public static ISerializer<FindSuccessorReplyMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(FindSuccessorReplyMessage sampleMessage, ByteBuf out) throws IOException {
            out.writeLong(sampleMessage.requestId.getMostSignificantBits());
            out.writeLong(sampleMessage.requestId.getLeastSignificantBits());
            ChordKey.serializer.serialize(sampleMessage.key, out);
            out.writeInt(sampleMessage.successors.length);
            for (int i=0; i<sampleMessage.successors.length; i++)
                ChordNode.serializer.serialize(sampleMessage.successors[i], out);
        }

        @Override
        public FindSuccessorReplyMessage deserialize(ByteBuf in) throws IOException {
            UUID requestId = new UUID(in.readLong(), in.readLong());
            ChordKey key = ChordKey.serializer.deserialize(in);
            int numberOfSuccessors = in.readInt();
            ChordNode[] successors = new ChordNode[numberOfSuccessors];
            for (int i=0; i<numberOfSuccessors; i++)
                successors[i] = ChordNode.serializer.deserialize(in);
            return new FindSuccessorReplyMessage(requestId, key, successors);
        }
    };
}




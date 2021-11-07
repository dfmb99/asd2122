package protocols.dht.chord.messages.search;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import protocols.dht.chord.types.ChordNode;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.io.IOException;
import java.math.BigInteger;
import java.util.UUID;

public class FindSuccessorReplyMessage extends ProtoMessage {

    public final static short MSG_ID = 209;

    private final UUID requestId;
    private final BigInteger key;
    private final ChordNode successor;

    public FindSuccessorReplyMessage(UUID requestId, BigInteger key, ChordNode successor) {
        super(MSG_ID);
        this.requestId = requestId;
        this.key = key;
        this.successor = successor;
    }

    public UUID getRequestId() {
        return requestId;
    }

    public BigInteger getKey() {
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
            byte[] keyBytes = sampleMessage.key.toByteArray();
            out.writeInt(keyBytes.length);
            out.writeBytes(keyBytes);
            ChordNode.serializer.serialize(sampleMessage.getSuccessor(), out);
        }

        @Override
        public FindSuccessorReplyMessage deserialize(ByteBuf in) throws IOException {
            UUID requestId = new UUID(in.readLong(), in.readLong());
            BigInteger key = new BigInteger(ByteBufUtil.getBytes(in.readBytes(in.readInt())));
            ChordNode successor = ChordNode.serializer.deserialize(in);
            return new FindSuccessorReplyMessage(requestId, key, successor);
        }
    };
}




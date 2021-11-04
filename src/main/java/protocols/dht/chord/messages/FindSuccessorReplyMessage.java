package protocols.dht.chord.messages;

import io.netty.buffer.ByteBuf;
import protocols.dht.chord.types.Node;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.io.IOException;
import java.math.BigInteger;
import java.util.UUID;

public class FindSuccessorReplyMessage extends ProtoMessage {

    public final static short MSG_ID = 102;

    private final UUID requestId;
    private final BigInteger fullKey;
    private final Node successor;

    public FindSuccessorReplyMessage(UUID requestId, BigInteger fullKey, Node successor) {
        super(MSG_ID);
        this.requestId = requestId;
        this.fullKey = fullKey;
        this.successor = successor;
    }

    public UUID getRequestId() {
        return requestId;
    }

    public BigInteger getFullKey() {
        return fullKey;
    }

    public Node getSuccessor() {
        return successor;
    }

    @Override
    public String toString() {
        return "FindSuccessorReplyMessage{" +
                "requestId=" + requestId.toString() +
                "fullKey=" + fullKey.toString() +
                "successor=" + successor.toString() +
                '}';
    }

    public static ISerializer<FindSuccessorReplyMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(FindSuccessorReplyMessage sampleMessage, ByteBuf out) throws IOException {
            out.writeLong(sampleMessage.requestId.getMostSignificantBits());
            out.writeLong(sampleMessage.requestId.getLeastSignificantBits());
            byte[] keyBytes = sampleMessage.fullKey.toByteArray();
            out.writeInt(keyBytes.length);
            out.writeBytes(keyBytes);
            Node.serializer.serialize(sampleMessage.getSuccessor(), out);
        }

        @Override
        public FindSuccessorReplyMessage deserialize(ByteBuf in) throws IOException {
            UUID requestId = new UUID(in.readLong(), in.readLong());
            BigInteger key = new BigInteger(in.readBytes(in.readInt()).array());
            Node successor = Node.serializer.deserialize(in);
            return new FindSuccessorReplyMessage(requestId, key, successor);
        }
    };
}




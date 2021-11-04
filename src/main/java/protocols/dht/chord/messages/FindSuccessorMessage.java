package protocols.dht.chord.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.math.BigInteger;
import java.util.UUID;

public class FindSuccessorMessage extends ProtoMessage {

    public final static short MSG_ID = 101;

    private final UUID requestId;
    private final BigInteger key;
    private final Host host;

    public FindSuccessorMessage(UUID requestId, BigInteger key, Host host) {
        super(MSG_ID);
        this.requestId = requestId;
        this.key = key;
        this.host = host;
    }

    public UUID getRequestId() {
        return requestId;
    }

    public BigInteger getKey() {
        return this.key;
    }

    public Host getHost() {
        return this.host;
    }

    @Override
    public String toString() {
        return "FindSuccessorMessage{" +
                "requestId=" + requestId.toString() +
                "key=" + key.toString() +
                "host=" + host.toString() +
                '}';
    }

    public static ISerializer<FindSuccessorMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(FindSuccessorMessage sampleMessage, ByteBuf out) throws IOException {
            out.writeLong(sampleMessage.requestId.getMostSignificantBits());
            out.writeLong(sampleMessage.requestId.getLeastSignificantBits());
            byte[] keyBytes = sampleMessage.key.toByteArray();
            out.writeInt(keyBytes.length);
            out.writeBytes(keyBytes);
            Host.serializer.serialize(sampleMessage.host, out);
        }

        @Override
        public FindSuccessorMessage deserialize(ByteBuf in) throws IOException {
            UUID requestId = new UUID(in.readLong(), in.readLong());
            BigInteger key = new BigInteger(in.readBytes(in.readInt()).array());
            Host host = Host.serializer.deserialize(in);
            return new FindSuccessorMessage(requestId, key, host);
        }
    };
}




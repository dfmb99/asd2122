package protocols.dht.chord.messages.search;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import protocols.dht.chord.ChordProtocol;
import protocols.dht.chord.types.ChordKey;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.math.BigInteger;
import java.util.UUID;

public class FindSuccessorMessage extends ProtoMessage {

    public final static short MSG_ID = 208;

    private final UUID requestId;
    private final ChordKey key;
    private final Host host;

    public FindSuccessorMessage(UUID requestId, ChordKey key, Host host) {
        super(MSG_ID);
        this.requestId = requestId;
        this.key = key;
        this.host = host;
    }

    public UUID getRequestId() {
        return requestId;
    }

    public ChordKey getKey() {
        return this.key;
    }

    public Host getHost() {
        return this.host;
    }

    @Override
    public String toString() {
        return "FindSuccessorMessage{" +
                "requestId=" + requestId +
                ", key=" + key +
                ", host=" + host +
                '}';
    }

    public static ISerializer<FindSuccessorMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(FindSuccessorMessage sampleMessage, ByteBuf out) throws IOException {
            out.writeLong(sampleMessage.requestId.getMostSignificantBits());
            out.writeLong(sampleMessage.requestId.getLeastSignificantBits());
            ChordKey.serializer.serialize(sampleMessage.key, out);
            Host.serializer.serialize(sampleMessage.host, out);
            ChordProtocol.logger.info("Message sent with size {}", out.readableBytes());
        }

        @Override
        public FindSuccessorMessage deserialize(ByteBuf in) throws IOException {
            ChordProtocol.logger.info("Message received with size {}", in.readableBytes());
            UUID uid = new UUID(in.readLong(), in.readLong());
            ChordKey key = ChordKey.serializer.deserialize(in);
            Host host = Host.serializer.deserialize(in);
            return new FindSuccessorMessage(uid, key, host);
        }
    };
}




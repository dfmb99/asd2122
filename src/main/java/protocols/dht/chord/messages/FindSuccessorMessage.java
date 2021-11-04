package protocols.dht.chord.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.math.BigInteger;

public class FindSuccessorMessage extends ProtoMessage {

    public final static short MSG_ID = 101;

    private final BigInteger fullKey;
    private final Host host; // process who issued the LookUp request

    public FindSuccessorMessage(BigInteger fullKey, Host host) {
        super(MSG_ID);
        this.fullKey = fullKey;
        this.host = host;
    }

    public BigInteger getFullKey() {
        return this.fullKey;
    }

    public BigInteger getKey(int m) {
        return this.fullKey.shiftRight(fullKey.bitLength() - m);
    }

    public Host getHost() {
        return this.host;
    }

    @Override
    public String toString() {
        return "FindSuccessorMessage{" +
                "fullKey=" + fullKey +
                "host=" + host +
                '}';
    }

    public static ISerializer<FindSuccessorMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(FindSuccessorMessage sampleMessage, ByteBuf out) throws IOException {
            byte[] keyBytes = sampleMessage.fullKey.toByteArray();
            out.writeInt(keyBytes.length);
            out.writeBytes(keyBytes);
            Host.serializer.serialize(sampleMessage.host, out);
        }

        @Override
        public FindSuccessorMessage deserialize(ByteBuf in) throws IOException {
            BigInteger key = new BigInteger(in.readBytes(in.readInt()).array());
            Host host = Host.serializer.deserialize(in);
            return new FindSuccessorMessage(key, host);
        }
    };
}




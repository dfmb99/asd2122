package protocols.dht.chord.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.math.BigInteger;

public class FindSuccessorMessage extends ProtoMessage {

    public final static short MSG_ID = 105;

    private final BigInteger key;
    private final Host host; // process who issued the LookUp request

    public FindSuccessorMessage(BigInteger key, Host host) {
        super(MSG_ID);
        this.key = key;
        this.host = host;
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
                "key=" + key +
                "host=" + host +
                '}';
    }

    public static ISerializer<FindSuccessorMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(FindSuccessorMessage sampleMessage, ByteBuf out) throws IOException {
            byte[] keyBytes = sampleMessage.key.toByteArray();
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




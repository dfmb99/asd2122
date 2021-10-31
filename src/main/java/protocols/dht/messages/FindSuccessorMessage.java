package protocols.dht.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.io.Serializable;
import java.math.BigInteger;
import java.util.HashSet;
import java.util.Set;

public class FindSuccessorMessage extends ProtoMessage {

    public final static short MSG_ID = 103;

    private final BigInteger key;
    private final Host host;    // process who issued the LookUp request

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
        return "GetSuccessorMessage{" +
                "key=" + key +
                "host=" + host +
                '}';
    }

    public static ISerializer<FindSuccessorMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(FindSuccessorMessage sampleMessage, ByteBuf out) throws IOException {
            Host.serializer.serialize(sampleMessage.host, out);
            byte[] keyBytes = sampleMessage.key.toByteArray();
            out.writeInt(keyBytes.length);
            out.writeBytes(keyBytes);
        }

        @Override
        public FindSuccessorMessage deserialize(ByteBuf in) throws IOException {
            Host host = Host.serializer.deserialize(in);
            BigInteger key = new BigInteger(in.readBytes(in.readInt()).array());
            return new FindSuccessorMessage(key, host);
        }
    };
}




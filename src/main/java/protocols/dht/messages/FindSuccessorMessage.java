package protocols.dht.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.math.BigInteger;

public class FindSuccessorMessage extends ProtoMessage {

    public final static short MSG_ID = 103;

    private final BigInteger id;
    private final Host host;

    public FindSuccessorMessage(BigInteger id, Host host) {
        super(MSG_ID);
        this.id = id;
        this.host = host;
    }

    public BigInteger getNodeId() {
        return this.id;
    }

    public Host getHost() {
        return this.host;
    }

    @Override
    public String toString() {
        return "GetSuccessorMessage{" +
                "id=" + id +
                "host=" + host +
                '}';
    }

    public static ISerializer<FindSuccessorMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(FindSuccessorMessage sampleMessage, ByteBuf out) throws IOException {
            // TODO: implement
        }

        @Override
        public FindSuccessorMessage deserialize(ByteBuf in) throws IOException {
            // TODO: implement
            return null;
        }
    };
}




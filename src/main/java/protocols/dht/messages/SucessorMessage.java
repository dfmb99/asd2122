package protocols.dht.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;

public class SucessorMessage extends ProtoMessage {

    public final static short MSG_ID = 103;

    private final int id;
    private final Host host;

    public SucessorMessage(int id, Host host) {
        super(MSG_ID);
        this.id = id;
        this.host = host;
    }

    public int getNodeId() {
        return this.id;
    }

    public Host getHost() {
        return this.host;
    }

    @Override
    public String toString() {
        return "GetSucessorMessage{" +
                "id=" + id +
                "host=" + host +
                '}';
    }

    public static ISerializer<SucessorMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(SucessorMessage sampleMessage, ByteBuf out) throws IOException {
            // TODO: implement
        }

        @Override
        public SucessorMessage deserialize(ByteBuf in) throws IOException {
            // TODO: implement
            return null;
        }
    };
}




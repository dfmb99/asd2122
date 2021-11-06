package protocols.dht.chord.messages.overlay;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.io.IOException;

public class KeepAliveMessage extends ProtoMessage {

    public final static short MSG_ID = 204;

    public KeepAliveMessage() {
        super(MSG_ID);
    }

    @Override
    public String toString() {
        return "KeepAliveMessage";
    }

    public static ISerializer<KeepAliveMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(KeepAliveMessage sampleMessage, ByteBuf out) {}

        @Override
        public KeepAliveMessage deserialize(ByteBuf in) {
            return new KeepAliveMessage();
        }
    };
}

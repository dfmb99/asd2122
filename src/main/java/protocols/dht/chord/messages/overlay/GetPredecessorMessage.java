package protocols.dht.chord.messages.overlay;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.io.IOException;

public class GetPredecessorMessage extends ProtoMessage {

    public final static short MSG_ID = 200;

    public GetPredecessorMessage() {
        super(MSG_ID);
    }

    @Override
    public String toString() {
        return "GetPredecessorMessage";
    }

    public static ISerializer<GetPredecessorMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(GetPredecessorMessage sampleMessage, ByteBuf out) {}

        @Override
        public GetPredecessorMessage deserialize(ByteBuf in) {
            return new GetPredecessorMessage();
        }
    };
}

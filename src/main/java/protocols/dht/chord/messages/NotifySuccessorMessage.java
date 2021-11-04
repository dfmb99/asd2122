package protocols.dht.chord.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.io.IOException;

public class NotifySuccessorMessage extends ProtoMessage {

    public final static short MSG_ID = 107;

    public NotifySuccessorMessage() {
        super(MSG_ID);
    }

    @Override
    public String toString() {
        return "GetPredecessorMessage";
    }

    public static ISerializer<NotifySuccessorMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(NotifySuccessorMessage sampleMessage, ByteBuf out) throws IOException {
        }

        @Override
        public NotifySuccessorMessage deserialize(ByteBuf in) throws IOException {
            return new NotifySuccessorMessage();
        }
    };
}

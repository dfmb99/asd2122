package protocols.dht.chord.messages.overlay;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.io.IOException;

public class NotifySuccessorMessage extends ProtoMessage {

    public final static short MSG_ID = 205;

    public NotifySuccessorMessage() {
        super(MSG_ID);
    }

    @Override
    public String toString() {
        return "NotifySuccessorMessage";
    }

    public static ISerializer<NotifySuccessorMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(NotifySuccessorMessage sampleMessage, ByteBuf out) {
        }

        @Override
        public NotifySuccessorMessage deserialize(ByteBuf in) {
            return new NotifySuccessorMessage();
        }
    };
}

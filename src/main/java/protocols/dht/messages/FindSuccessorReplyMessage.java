package protocols.dht.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;

public class FindSuccessorReplyMessage extends ProtoMessage {

    public final static short MSG_ID = 104;

    private final Host successor;


    public FindSuccessorReplyMessage(Host successor) {
        super(MSG_ID);
        this.successor = successor;
    }

    public Host getSuccessor() {
        return this.successor;
    }

    @Override
    public String toString() {
        return "GetSuccessorMessage{" +
                "successor=" + successor.toString() +
                '}';
    }

    public static ISerializer<FindSuccessorReplyMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(FindSuccessorReplyMessage sampleMessage, ByteBuf out) throws IOException {
            // TODO: implement
        }

        @Override
        public FindSuccessorReplyMessage deserialize(ByteBuf in) throws IOException {
            // TODO: implement
            return null;
        }
    };
}




package protocols.dht.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.math.BigInteger;

public class FindSuccessorReplyMessage extends ProtoMessage {

    public final static short MSG_ID = 104;

    private final BigInteger key;    // id that was requested
    private final Host successor;   // the successor of the requested id


    public FindSuccessorReplyMessage(Host successor, BigInteger key) {
        super(MSG_ID);
        this.successor = successor;
        this.key = key;
    }

    public Host getSuccessor() {
        return this.successor;
    }

    public BigInteger getKey() {
        return this.key;
    }

    @Override
    public String toString() {
        return "GetSuccessorMessage{" +
                "key=" + key +
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




package protocols.dht.chord.messages;

import io.netty.buffer.ByteBuf;
import protocols.dht.chord.types.Node;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.io.IOException;
import java.math.BigInteger;

public class FindSuccessorReplyMessage extends ProtoMessage {

    public final static short MSG_ID = 106;

    private final BigInteger key;
    private final Node successor;

    public FindSuccessorReplyMessage(BigInteger key, Node successor) {
        super(MSG_ID);
        this.key = key;
        this.successor = successor;
    }

    public BigInteger getKey() {
        return key;
    }

    public Node getSuccessor() {
        return successor;
    }

    @Override
    public String toString() {
        return "FindSuccessorReplyMessage{" +
                "key=" + key.toString() +
                "successor=" + successor.toString() +
                '}';
    }

    public static ISerializer<FindSuccessorReplyMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(FindSuccessorReplyMessage msg, ByteBuf out) throws IOException {
            byte[] keyBytes = msg.key.toByteArray();
            out.writeInt(keyBytes.length);
            out.writeBytes(keyBytes);
            Node.serializer.serialize(msg.getSuccessor(), out);
        }

        @Override
        public FindSuccessorReplyMessage deserialize(ByteBuf in) throws IOException {
            BigInteger key = new BigInteger(in.readBytes(in.readInt()).array());
            Node successor = Node.serializer.deserialize(in);
            return new FindSuccessorReplyMessage(key, successor);
        }
    };
}




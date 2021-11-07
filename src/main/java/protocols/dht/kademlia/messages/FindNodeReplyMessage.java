package protocols.dht.kademlia.messages;

import io.netty.buffer.ByteBuf;
import protocols.dht.kademlia.types.Node;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.io.IOException;

public class FindNodeReplyMessage extends ProtoMessage {

    public final static short MSG_ID = 401;

    private Node node;

    public FindNodeReplyMessage(Node node) {
        super(MSG_ID);
        this.node = node;
    }

    public Node getNode() {
        return this.node;
    }

    @Override
    public String toString() {
        return "FindNodeMessageReply{" +
                "node=" + node.toString() +
                '}';
    }

    public static ISerializer<FindNodeReplyMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(FindNodeReplyMessage sampleMessage, ByteBuf out) throws IOException {
            Node.serializer.serialize(sampleMessage.node, out);
        }

        @Override
        public FindNodeReplyMessage deserialize(ByteBuf in) throws IOException {
            Node node = Node.serializer.deserialize(in);
            return new FindNodeReplyMessage(node);
        }
    };
}

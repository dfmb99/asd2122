package protocols.dht.chord.messages;

import io.netty.buffer.ByteBuf;
import protocols.dht.chord.types.Node;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.math.BigInteger;

public class JoinRingMessage extends ProtoMessage {

    public final static short MSG_ID = 105;

    Node node;

    public JoinRingMessage(Node node) {
        super(MSG_ID);
        this.node = node;
    }

    public Node getNode() {
        return node;
    }

    @Override
    public String toString() {
        return "JoinRingMessage{" +
                "node=" + node.toString() +
                '}';
    }

    public static ISerializer<JoinRingMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(JoinRingMessage sampleMessage, ByteBuf out) throws IOException {
            Node.serializer.serialize(sampleMessage.node, out);
        }

        @Override
        public JoinRingMessage deserialize(ByteBuf in) throws IOException {
            Node node = Node.serializer.deserialize(in);
            return new JoinRingMessage(node);
        }
    };
}




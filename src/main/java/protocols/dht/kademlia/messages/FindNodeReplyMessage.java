package protocols.dht.kademlia.messages;

import io.netty.buffer.ByteBuf;
import protocols.dht.kademlia.types.Node;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.io.IOException;

public class FindNodeReplyMessage extends ProtoMessage {

    public final static short MSG_ID = 401;

    private Node[] closestNodes;

    public FindNodeReplyMessage(Node[] closestNodes) {
        super(MSG_ID);
        this.closestNodes = closestNodes;
    }

    public Node[] getClosestNodes() {
        return this.closestNodes;
    }

    @Override
    public String toString() {
        String res = "FindNodeMessageReply{ ";
        for (Node closestNode : closestNodes) {
            res = res.concat("node=" + closestNode.toString() + " ");
        }
        return res.concat("}");
    }

    public static ISerializer<FindNodeReplyMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(FindNodeReplyMessage sampleMessage, ByteBuf out) throws IOException {
            out.writeInt(sampleMessage.getClosestNodes().length);
            for(Node n: sampleMessage.getClosestNodes()){
                Node.serializer.serialize(n, out);
            }
        }

        @Override
        public FindNodeReplyMessage deserialize(ByteBuf in) throws IOException {
            int size = in.readInt();
            Node[] closestNodes = new Node[size];
            for(int i = 0; i < size; i++){
                closestNodes[i] = Node.serializer.deserialize(in);
            }
            return new FindNodeReplyMessage(closestNodes);
        }
    };
}

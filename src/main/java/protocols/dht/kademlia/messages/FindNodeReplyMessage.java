package protocols.dht.kademlia.messages;

import io.netty.buffer.ByteBuf;
import protocols.dht.kademlia.types.KademliaNode;
import protocols.dht.kademlia.types.Node;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.io.IOException;
import java.math.BigInteger;
import java.util.SortedSet;
import java.util.TreeSet;

public class FindNodeReplyMessage extends ProtoMessage {

    public final static short MSG_ID = 401;

    private BigInteger id;  // id whose closestNodes are referring to
    private SortedSet<KademliaNode> closestNodes;

    public FindNodeReplyMessage(BigInteger id, SortedSet<KademliaNode> closestNodes) {
        super(MSG_ID);
        this.id = id;
        this.closestNodes = closestNodes;
    }

    public BigInteger getLookupId(){
        return this.id;
    }

    public SortedSet<KademliaNode> getClosestNodes() {
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
            byte[] idBytes = sampleMessage.getLookupId().toByteArray();
            out.writeInt(idBytes.length);
            out.writeBytes(idBytes);
            out.writeInt(sampleMessage.getClosestNodes().size());
            for(KademliaNode n: sampleMessage.getClosestNodes()){
                KademliaNode.serializer.serialize(n, out);
            }
        }

        @Override
        public FindNodeReplyMessage deserialize(ByteBuf in) throws IOException {
            BigInteger id = new BigInteger(in.readBytes(in.readInt()).array());
            int size = in.readInt();
            SortedSet<KademliaNode> closestNodes = new TreeSet<>();
            for(int i = 0; i < size; i++){
                closestNodes.add(KademliaNode.serializer.deserialize(in));
            }
            return new FindNodeReplyMessage(id, closestNodes);
        }
    };
}

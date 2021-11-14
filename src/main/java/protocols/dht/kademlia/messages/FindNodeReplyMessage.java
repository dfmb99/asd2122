package protocols.dht.kademlia.messages;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import protocols.dht.kademlia.KademliaProtocol;
import protocols.dht.kademlia.types.KademliaNode;
import protocols.dht.types.Node;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.io.IOException;
import java.math.BigInteger;
import java.util.SortedSet;
import java.util.TreeSet;

public class FindNodeReplyMessage extends ProtoMessage {

    public final static short MSG_ID = 301;

    private BigInteger id;  // id whose closestNodes are referring to
    private SortedSet<KademliaNode> closestNodes;
    private boolean isBootstrapping;

    public FindNodeReplyMessage(BigInteger id, SortedSet<KademliaNode> closestNodes, boolean isBootstrapping) {
        super(MSG_ID);
        this.id = id;
        this.closestNodes = closestNodes;
        this.isBootstrapping = isBootstrapping;
    }

    public BigInteger getLookupId(){
        return this.id;
    }

    public boolean isBootstrapping() {
        return isBootstrapping;
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
            out.writeBoolean(sampleMessage.isBootstrapping());
            KademliaProtocol.logger.info("Message sent with size {}", out.readableBytes());
        }

        @Override
        public FindNodeReplyMessage deserialize(ByteBuf in) throws IOException {
            KademliaProtocol.logger.info("Message received with size {}", in.readableBytes());
            BigInteger id = new BigInteger(ByteBufUtil.getBytes(in.readBytes(in.readInt())));
            int size = in.readInt();
            SortedSet<KademliaNode> closestNodes = new TreeSet<>();
            for(int i = 0; i < size; i++){
                closestNodes.add(KademliaNode.serializer.deserialize(in));
            }
            boolean bootstrapping = in.readBoolean();
            return new FindNodeReplyMessage(id, closestNodes, bootstrapping);
        }
    };
}

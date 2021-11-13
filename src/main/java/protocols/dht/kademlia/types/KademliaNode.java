package protocols.dht.kademlia.types;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import protocols.dht.types.Node;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.math.BigInteger;


/**
 * This class is used to maintain nodes ordered in a set.
 *
 * The metric used to order the set is the distance to a certain id, which is the id
 * being looked for in the context of the creation of this node
 */
public class KademliaNode extends Node implements Comparable<KademliaNode>{

    private final BigInteger distance;    // xor distance to the id we are searching for

    /**
     * This constructor calculates a xor distance to the received id
     * @param host - node's contact information
     * @param idToCompare - id to use in the distance calculation
     */
    public KademliaNode(Host host, BigInteger idToCompare) {
        super(host);
        this.distance = calcXorDistance(idToCompare);
    }

    /**
     * @param distance - distance to a given node being lookedUp
     * @param host - node's contact information
     */
    public KademliaNode(BigInteger distance, Host host){
        super(host);
        this.distance = distance;
    }

    private BigInteger calcXorDistance(BigInteger idToCompare) {
        return this.getId().xor(idToCompare);
    }

    private BigInteger getDistance(){
        return distance;
    }

    @Override
    public int compareTo(KademliaNode other) {
        return this.distance.compareTo(other.getDistance());
    }

    public static ISerializer<KademliaNode> serializer = new ISerializer<KademliaNode>() {
        public void serialize(KademliaNode node, ByteBuf out) throws IOException {
            Host.serializer.serialize(node.getHost(), out);
            byte[] distBytes = node.getDistance().toByteArray();
            out.writeInt(distBytes.length);
            out.writeBytes(distBytes);
        }

        public KademliaNode deserialize(ByteBuf in) throws IOException {
            Host host = Host.serializer.deserialize(in);
            BigInteger distance = new BigInteger(ByteBufUtil.getBytes(in.readBytes(in.readInt())));
            return new KademliaNode(distance, host);
        }
    };

}

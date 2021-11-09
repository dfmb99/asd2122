package protocols.dht.kademlia.types;

import pt.unl.fct.di.novasys.network.data.Host;

import java.math.BigInteger;

public class KademliaNode extends Node implements Comparable<KademliaNode>{

    private final BigInteger distance;    // xor distance to the id we are searching for

    public KademliaNode(Host host, BigInteger idToCompare) {
        super(host);
        this.distance = calcDistance(idToCompare);
    }

    private BigInteger calcDistance(BigInteger idToCompare) {
        return this.getId().xor(idToCompare);
    }

    private BigInteger getDistance(){
        return distance;
    }

    @Override
    public int compareTo(KademliaNode other) {
        return this.distance.compareTo(other.getDistance());
    }
}

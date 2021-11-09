package protocols.dht.kademlia.types;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;
import utils.HashGenerator;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Objects;

public class Node {

    private final BigInteger id;
    private final Host host;

    public Node(Host host) {
        this.id = HashGenerator.generateHash(host.toString());
        this.host = host;
    }

    public BigInteger getId() {
        return id;
    }

    public Host getHost() {
        return host;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Node)) return false;
        Node node = (Node) o;
        return Objects.equals(getHost(), node.getHost());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getHost());
    }

    public static ISerializer<Node> serializer = new ISerializer<Node>() {
        public void serialize(Node node, ByteBuf out) throws IOException {
            Host.serializer.serialize(node.getHost(), out);
        }

        public Node deserialize(ByteBuf in) throws IOException {
            Host host = Host.serializer.deserialize(in);
            return new Node(host);
        }
    };
}

package protocols.dht.chord.types;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;
import utils.HashGenerator;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Objects;

public class Node {

    private final BigInteger id;
    private final Host host;

    public Node(Host host, int m) {
        BigInteger hash = HashGenerator.generateHash(host.toString()).abs();
        this.id = hash.shiftRight(hash.bitLength() - m);
        this.host = host;
    }

    public Node(BigInteger id, Host host) {
        this.id = id;
        this.host = host;
    }

    public BigInteger getId() {
        return id;
    }

    public Host getHost() {
        return host;
    }

    @Override
    public String toString() {
        return "Node{" +
                "id=" + id +
                ", host=" + host +
                '}';
    }

    public static ISerializer<Node> serializer = new ISerializer<>() {
        public void serialize(Node node, ByteBuf out) throws IOException {
            byte[] idBytes = node.id.toByteArray();
            out.writeInt(idBytes.length);
            out.writeBytes(idBytes);
            Host.serializer.serialize(node.getHost(), out);
        }

        public Node deserialize(ByteBuf in) throws IOException {
            BigInteger id = new BigInteger(ByteBufUtil.getBytes(in.readBytes(in.readInt())));
            Host host = Host.serializer.deserialize(in);
            return new Node(id, host);
        }
    };

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Node)) return false;
        Node node = (Node) o;
        return id.equals(node.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
}

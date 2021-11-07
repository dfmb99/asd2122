package protocols.dht.chord.types;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Objects;

public class ChordNode {

    private final ChordKey id;
    private final Host host;

    public ChordNode(Host host) {
        this.id = ChordKey.of(host.toString());
        this.host = host;
    }

    private ChordNode(ChordKey id, Host host) {
        this.id = id;
        this.host = host;
    }

    public ChordKey getId() {
        return id;
    }

    public Host getHost() {
        return host;
    }

    public static boolean equals(ChordNode first, ChordNode second) {
        if(first == null) return false;
        else return first.equals(second);
    }

    public static boolean equals(ChordNode first, Host second) {
        if(first == null) return false;
        else return first.host.equals(second);
    }

    public static boolean equals(Host first, ChordNode second) {
        if(first == null) return false;
        else return first.equals(second.host);
    }

    public static boolean equals(Host first, Host second) {
        if(first == null) return false;
        else return first.equals(second);
    }

    @Override
    public String toString() {
        return "Node{" +
                "id=" + id +
                ", host=" + host +
                '}';
    }

    public static ISerializer<ChordNode> serializer = new ISerializer<>() {
        public void serialize(ChordNode node, ByteBuf out) throws IOException {
            ChordKey.serializer.serialize(node.id, out);
            Host.serializer.serialize(node.host, out);
        }

        public ChordNode deserialize(ByteBuf in) throws IOException {
            ChordKey id = ChordKey.serializer.deserialize(in);
            Host host = Host.serializer.deserialize(in);
            return new ChordNode(id, host);
        }
    };

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ChordNode)) return false;
        ChordNode node = (ChordNode) o;
        return host.equals(node.host);
    }

    @Override
    public int hashCode() {
        return Objects.hash(host);
    }
}

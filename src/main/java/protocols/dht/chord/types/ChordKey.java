package protocols.dht.chord.types;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import protocols.dht.chord.ChordProtocol;
import pt.unl.fct.di.novasys.network.ISerializer;
import utils.HashGenerator;

import java.math.BigInteger;

public class ChordKey implements Comparable<ChordKey> {

    public final BigInteger id;

    public ChordKey(String seed) {
        BigInteger hash = HashGenerator.generateHash(seed).abs();
        id = hash.shiftRight(Math.max(0, hash.bitLength() - 160));
    }

    public ChordKey(ChordSegment segment) {
        this.id = segment.ringPosition;
    }

    public ChordKey(BigInteger id) {
        this.id = id;
    }

    public ChordKey getKeyAfterRingLoop() {
        return new ChordKey(id.add(BigInteger.TWO.pow(160)));
    }

    @Override
    public int compareTo(ChordKey other) {
        return id.compareTo(other.id);
    }

    @Override
    public String toString() {
        return "ChordKey{" +
                "id=" + id +
                '}';
    }

    public static ISerializer<ChordKey> serializer = new ISerializer<>() {
        public void serialize(ChordKey node, ByteBuf out) {
            byte[] idBytes = node.id.toByteArray();
            out.writeInt(idBytes.length);
            out.writeBytes(idBytes);
        }

        public ChordKey deserialize(ByteBuf in) {
            BigInteger id = new BigInteger(ByteBufUtil.getBytes(in.readBytes(in.readInt())));
            return new ChordKey(id);
        }
    };

}

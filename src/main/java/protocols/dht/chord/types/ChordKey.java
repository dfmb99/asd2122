package protocols.dht.chord.types;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import protocols.dht.chord.ChordProtocol;
import pt.unl.fct.di.novasys.network.ISerializer;
import utils.HashGenerator;

import java.math.BigInteger;

public class ChordKey implements Comparable<ChordKey> {

    public final BigInteger full;
    public final BigInteger ringPosition;

    public static ChordKey of(String seed) {
        BigInteger full = HashGenerator.generateHash(seed).abs();
        BigInteger compact = full.shiftRight(full.bitLength() - ChordProtocol.M);
        return new ChordKey(full,compact);
    }

    public static ChordKey of(ChordSegment segment) {
        return new ChordKey(BigInteger.ZERO,segment.ringPosition);
    }

    private ChordKey(BigInteger full, BigInteger ringPosition) {
        this.full = full;
        this.ringPosition = ringPosition;
    }

    public ChordKey getKeyAfterRingLoop(BigInteger ringSize) {
        return new ChordKey(full, ringPosition.add(ringSize));
    }

    @Override
    public int compareTo(ChordKey other) {
        int e = ringPosition.compareTo(other.ringPosition);
        if(e == 0)
            return full.compareTo(other.full);
        else
            return e;
    }

    @Override
    public String toString() {
        return "ChordKey{" +
                "full=" + full +
                ", ringPosition=" + ringPosition +
                '}';
    }

    public static ISerializer<ChordKey> serializer = new ISerializer<>() {
        public void serialize(ChordKey node, ByteBuf out) {
            byte[] full = node.full.toByteArray();
            out.writeInt(full.length);
            out.writeBytes(full);
        }

        public ChordKey deserialize(ByteBuf in) {
            BigInteger full = new BigInteger(ByteBufUtil.getBytes(in.readBytes(in.readInt())));
            BigInteger ringPosition = full.shiftRight(full.bitLength() - ChordProtocol.M);
            return new ChordKey(full, ringPosition);
        }
    };

}

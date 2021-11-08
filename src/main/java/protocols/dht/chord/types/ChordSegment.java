package protocols.dht.chord.types;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.math.BigInteger;

public class ChordSegment implements Comparable<ChordSegment> {

    public final BigInteger ringPosition;
    public final int fingerIndex;

    public static ChordSegment of(ChordKey node, int fingerIndex) {
        return new ChordSegment(node.ringPosition.add(BigInteger.TWO.pow(fingerIndex)), fingerIndex);
    }

    private ChordSegment(BigInteger ringPosition, int fingerIndex) {
        this.ringPosition = ringPosition;
        this.fingerIndex = fingerIndex;
    }

    @Override
    public int compareTo(ChordSegment other) {
        return ringPosition.compareTo(other.ringPosition);
    }

    @Override
    public String toString() {
        return "ChordSegment{" +
                "ringPosition=" + ringPosition +
                ", fingerIndex=" + fingerIndex +
                '}';
    }

    public static ISerializer<ChordSegment> serializer = new ISerializer<>() {
        public void serialize(ChordSegment segment, ByteBuf out) {
            byte[] ringLocationBytes = segment.ringPosition.toByteArray();
            out.writeInt(ringLocationBytes.length);
            out.writeBytes(ringLocationBytes);
            out.writeInt(segment.fingerIndex);
        }

        public ChordSegment deserialize(ByteBuf in) {
            BigInteger ringPosition = new BigInteger(ByteBufUtil.getBytes(in.readBytes(in.readInt())));
            int fingerIndex = in.readInt();
            return new ChordSegment(ringPosition, fingerIndex);
        }
    };

}

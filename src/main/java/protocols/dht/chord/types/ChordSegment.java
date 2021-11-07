package protocols.dht.chord.types;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.math.BigInteger;

public class ChordSegment implements Comparable<ChordSegment> {

    public final BigInteger ringLocation;
    public final int fingerIndex;

    public static ChordSegment of(ChordKey node, int fingerIndex) {
        return new ChordSegment(node.compact.add(BigInteger.TWO.pow(fingerIndex)), fingerIndex);
    }

    private ChordSegment(BigInteger ringLocation, int fingerIndex) {
        this.ringLocation = ringLocation;
        this.fingerIndex = fingerIndex;
    }

    @Override
    public int compareTo(ChordSegment other) {
        return ringLocation.compareTo(other.ringLocation);
    }

    @Override
    public String toString() {
        return "ChordSegment{" +
                "ringLocation=" + ringLocation +
                ", fingerIndex=" + fingerIndex +
                '}';
    }

    public static ISerializer<ChordSegment> serializer = new ISerializer<>() {
        public void serialize(ChordSegment segment, ByteBuf out) {
            byte[] ringLocationBytes = segment.ringLocation.toByteArray();
            out.writeInt(ringLocationBytes.length);
            out.writeBytes(ringLocationBytes);
            out.writeInt(segment.fingerIndex);
        }

        public ChordSegment deserialize(ByteBuf in) {
            BigInteger ringLocation = new BigInteger(ByteBufUtil.getBytes(in.readBytes(in.readInt())));
            int fingerIndex = in.readInt();
            return new ChordSegment(ringLocation, fingerIndex);
        }
    };

}

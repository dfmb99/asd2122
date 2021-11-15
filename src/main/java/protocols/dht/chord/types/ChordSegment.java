package protocols.dht.chord.types;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.math.BigInteger;

public class ChordSegment implements Comparable<ChordSegment> {

    public static int numberOfFingers;

    public final BigInteger ringPosition;
    public final int fingerIndex;

    public ChordSegment(ChordKey node, int fingerIndex) {
        this.ringPosition = (node.id
            .add(BigInteger.TWO.pow(80)))
                .mod(BigInteger.TWO.pow(160))
                    .divide(BigInteger.valueOf(numberOfFingers-fingerIndex));

        // finger_position = ((node_position + 2^80) % 2^160) / (number_finger - finger_index)

        this.fingerIndex = fingerIndex;
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
            byte[] ringPositionBytes = segment.ringPosition.toByteArray();
            out.writeInt(ringPositionBytes.length);
            out.writeBytes(ringPositionBytes);
            out.writeInt(segment.fingerIndex);
        }

        public ChordSegment deserialize(ByteBuf in) {
            byte[] h = new byte[in.readInt()];
            in.readBytes(h);
            BigInteger ringPosition = new BigInteger(h);
            int fingerIndex = in.readInt();
            return new ChordSegment(ringPosition, fingerIndex);
        }
    };

}

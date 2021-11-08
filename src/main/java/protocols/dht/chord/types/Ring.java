package protocols.dht.chord.types;

import java.math.BigInteger;

public class Ring {

    private final BigInteger ringSize;

    public Ring(BigInteger ringSize) {
        this.ringSize = ringSize;
    }



    public boolean inBounds(ChordKey key, ChordKey lowerBound, ChordKey upperBound) {
        int ul = upperBound.compareTo(lowerBound);
        if(ul == 0) {
            return true;
        }
        if(ul < 0) {
            if(key.compareTo(upperBound) <= 0)
                key = key.getKeyAfterRingLoop(ringSize);
            upperBound = upperBound.getKeyAfterRingLoop(ringSize);
        }

        return key.compareTo(lowerBound) > 0 && key.compareTo(upperBound) <= 0;
    }



    public boolean inBounds(ChordNode node, ChordNode lowerBound, ChordNode upperBound) {
        return inBounds(node.getId(), lowerBound.getId(), upperBound.getId());
    }

    public boolean inBounds(ChordKey key, ChordNode lowerBound, ChordNode upperBound) {
        return inBounds(key, lowerBound.getId(), upperBound.getId());
    }

    public boolean inBounds(ChordSegment segment, ChordNode lowerBound, ChordNode upperBound) {
        return inBounds(ChordKey.of(segment), lowerBound.getId(), upperBound.getId());
    }
}

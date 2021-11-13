package protocols.dht.chord.types;

public class Ring {


    public static boolean inBounds(ChordKey key, ChordKey lowerBound, ChordKey upperBound) {
        int ul = upperBound.compareTo(lowerBound);
        if(ul == 0) {
            return true;
        }
        if(ul < 0) {
            if(key.compareTo(upperBound) <= 0)
                key = key.getKeyAfterRingLoop();
            upperBound = upperBound.getKeyAfterRingLoop();
        }

        return key.compareTo(lowerBound) > 0 && key.compareTo(upperBound) <= 0;
    }



    public static boolean inBounds(ChordNode node, ChordNode lowerBound, ChordNode upperBound) {
        return inBounds(node.getId(), lowerBound.getId(), upperBound.getId());
    }

    public static boolean inBounds(ChordKey key, ChordNode lowerBound, ChordNode upperBound) {
        return inBounds(key, lowerBound.getId(), upperBound.getId());
    }

    public static boolean inBounds(ChordSegment segment, ChordNode lowerBound, ChordNode upperBound) {
        return inBounds(new ChordKey(segment), lowerBound.getId(), upperBound.getId());
    }
}

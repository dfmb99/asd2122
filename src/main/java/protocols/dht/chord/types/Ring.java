package protocols.dht.chord.types;

import java.math.BigInteger;

public class Ring {

    private final BigInteger ringSize;

    public Ring(BigInteger ringSize) {
        this.ringSize = ringSize;
    }

    public boolean InBounds(BigInteger value, BigInteger lowerBound, BigInteger upperBound) {
        if(lowerBound.compareTo(upperBound) == 0) {
            return true;
        }
        else if(upperBound.compareTo(lowerBound) < 0) {
            if(value.compareTo(upperBound) <= 0)
                value = value.add(ringSize);
            return value.compareTo(lowerBound) >= 0 && value.compareTo(upperBound.add(ringSize)) <= 0;
        }
        else {
            return value.compareTo(lowerBound) >= 0 && value.compareTo(upperBound) <= 0;
        }
    }
}

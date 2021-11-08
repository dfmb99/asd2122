import protocols.dht.chord.ChordProtocol;
import protocols.dht.chord.types.ChordKey;
import protocols.dht.chord.types.Ring;

import java.math.BigInteger;

public class Test {

    public static void main(String[] args) throws Exception {
        ChordProtocol.M = 4;

        ChordKey val = ChordKey.of("127.0.0.1:8080");
        ChordKey lower = ChordKey.of("127.0.0.1:8081");
        ChordKey upper = ChordKey.of("127.0.0.1:8082");

        System.out.println(val.ringPosition);
        System.out.println(lower.ringPosition);
        System.out.println(upper.ringPosition);
        System.out.println(new Ring(BigInteger.TWO.pow(4)).inBounds(val,lower,upper));
    }
}

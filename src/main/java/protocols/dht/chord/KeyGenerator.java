package protocols.dht.chord;

import utils.HashGenerator;

import java.math.BigInteger;

public class KeyGenerator {
    public static BigInteger gen(String seed, int size) {
        BigInteger hash = HashGenerator.generateHash(seed).abs();
        return hash.shiftRight(hash.bitLength() - size);
    }
}

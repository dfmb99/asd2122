package protocols.dht.requests;

import java.math.BigInteger;
import java.util.UUID;

import pt.unl.fct.di.novasys.babel.generic.ProtoRequest;

public class LookupRequest extends ProtoRequest {

    public final static short REQUEST_ID = 101;

    private final BigInteger fullKey;
    private final UUID uid;

    public LookupRequest(BigInteger fullKey) {
        super(REQUEST_ID);
        this.fullKey = fullKey;
        this.uid = UUID.randomUUID();
    }

    public BigInteger getFullKey() {
        return fullKey;
    }

    public UUID getUid() {
        return uid;
    }
}
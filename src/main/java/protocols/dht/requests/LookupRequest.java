package protocols.dht.requests;

import java.math.BigInteger;
import java.util.UUID;

import pt.unl.fct.di.novasys.babel.generic.ProtoRequest;

public class LookupRequest extends ProtoRequest {

    public final static short REQUEST_ID = 101;

    private final String name;
    private final UUID uid;

    public LookupRequest(String name, UUID uid) {
        super(REQUEST_ID);
        this.name = name;
        this.uid = uid;
    }

    public String getName() {
        return name;
    }

    public UUID getUid() {
        return uid;
    }
}
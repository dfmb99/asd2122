package protocols.dht.requests;

import java.util.UUID;

import pt.unl.fct.di.novasys.babel.generic.ProtoRequest;

public class LookupRequest extends ProtoRequest {

    public final static short REQUEST_TYPE_ID = 200;

    private final UUID requestId;
    private final String name;

    public LookupRequest(UUID requestId, String name) {
        super(REQUEST_TYPE_ID);
        this.requestId = requestId;
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public UUID getRequestId() {
        return requestId;
    }
}
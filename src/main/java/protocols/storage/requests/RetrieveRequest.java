package protocols.storage.requests;

import java.util.UUID;

import pt.unl.fct.di.novasys.babel.generic.ProtoRequest;

public class RetrieveRequest extends ProtoRequest {

	final public static short REQUEST_TYPE_ID = 400;

	private final UUID requestId;
	private final String name;
	
	public RetrieveRequest(String name) {
		super(RetrieveRequest.REQUEST_TYPE_ID);
		this.requestId = UUID.randomUUID();
		this.name = name;
	}
	
	public UUID getRequestId() {
		return this.requestId;
	}
	
	public String getName() {
		return this.name;
	}

	@Override
	public String toString() {
		return "RetrieveRequest{" +
				"requestId=" + requestId +
				", name='" + name + '\'' +
				'}';
	}
}

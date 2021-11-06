package protocols.storage.requests;

import java.util.UUID;

import pt.unl.fct.di.novasys.babel.generic.ProtoRequest;

public class StoreRequest extends ProtoRequest {

	final public static short REQUEST_TYPE_ID = 401;

	private final UUID requestId;
	private final String name;
	private final byte[] content;
	
	public StoreRequest(String name, byte[] content) {
		super(StoreRequest.REQUEST_TYPE_ID);
		this.requestId = UUID.randomUUID();
		this.name = name;
		this.content = content;
	}
	
	public UUID getRequestId() {
		return this.requestId;
	}
	
	public String getName() {
		return this.name;
	}
	
	public byte[] getContent() {
		return this.content;
	}

}

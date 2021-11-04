package protocols.storage.requests;

import java.util.UUID;

import pt.unl.fct.di.novasys.babel.generic.ProtoRequest;

public class StoreRequest extends ProtoRequest {

	final public static short REQUEST_ID = 201;

	private UUID id;
	private String name;
	private byte[] content;
	
	public StoreRequest(String name, byte[] content) {
		super(StoreRequest.REQUEST_ID);
		this.id = UUID.randomUUID();
		this.name = name;
		this.content = content;
	}
	
	public UUID getRequestId() {
		return this.id;
	}
	
	public String getName() {
		return this.name;
	}
	
	public byte[] getContent() {
		return this.content;
	}

}

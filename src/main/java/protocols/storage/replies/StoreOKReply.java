package protocols.storage.replies;

import java.util.UUID;

import pt.unl.fct.di.novasys.babel.generic.ProtoReply;

public class StoreOKReply extends ProtoReply {

	final public static short REPLY_TYPE_ID = 402;

	private final UUID requestId;
	private final String name;
	
	public StoreOKReply(UUID requestId, String name) {
		super(StoreOKReply.REPLY_TYPE_ID);
		this.requestId = requestId;
		this.name = name;
	}

	public UUID getRequestId() {
		return requestId;
	}

	public String getName() {
		return this.name;
	}
	
}

package protocols.storage.replies;

import java.util.UUID;

import pt.unl.fct.di.novasys.babel.generic.ProtoReply;

public class RetrieveOKReply extends ProtoReply {

	final public static short REPLY_TYPE_ID = 401;

	private final UUID requestId;
	private final String name;
	private final byte[] content;
	
	public RetrieveOKReply(UUID requestId, String name, byte[] content) {
		super(RetrieveOKReply.REPLY_TYPE_ID);
		this.requestId = requestId;
		this.name = name;
		this.content = content;
	}

	public UUID getRequestId() {
		return requestId;
	}

	public String getName() {
		return name;
	}

	public byte[] getContent() {
		return content;
	}
}

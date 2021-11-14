package protocols.storage.replies;

import java.util.UUID;

import pt.unl.fct.di.novasys.babel.generic.ProtoReply;

public class RetrieveFailedReply extends ProtoReply {

	final public static short REPLY_TYPE_ID = 400;

	private final UUID requestId;
	private final String name;
	
	public RetrieveFailedReply(UUID requestId, String name) {
		super(RetrieveFailedReply.REPLY_TYPE_ID);
		this.requestId = requestId;
		this.name = name;
	}

	public UUID getRequestId() {
		return requestId;
	}

	public String getName() {
		return this.name;
	}

	@Override
	public String toString() {
		return "RetrieveFailedReply{" +
				"requestId=" + requestId +
				", name='" + name + '\'' +
				'}';
	}
}

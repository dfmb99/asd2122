package protocols.storage.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

public class RetrieveContentReplyMessage extends ProtoMessage {

    public final static short MSG_ID = 401;

    private final UUID requestId;
    private final String name;
    private final byte[] content;

    public RetrieveContentReplyMessage(UUID requestId, String name, byte[] content) {
        super(MSG_ID);
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

    @Override
    public String toString() {
        return "RetrieveContentReplyMessage{" +
                "requestId=" + requestId +
                "name=" + name +
                '}';
    }

    public static ISerializer<RetrieveContentReplyMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(RetrieveContentReplyMessage sampleMessage, ByteBuf out) {
            out.writeLong(sampleMessage.requestId.getMostSignificantBits());
            out.writeLong(sampleMessage.requestId.getLeastSignificantBits());
            byte[] nameBytes = StandardCharsets.ISO_8859_1.encode(sampleMessage.name).array();
            out.writeInt(nameBytes.length);
            out.writeBytes(nameBytes);
            out.writeInt(sampleMessage.content.length);
            out.writeBytes(sampleMessage.content);
        }

        @Override
        public RetrieveContentReplyMessage deserialize(ByteBuf in) {
            UUID requestId = new UUID(in.readLong(), in.readLong());
            String name = new String(in.readBytes(in.readInt()).array(), StandardCharsets.ISO_8859_1);
            byte[] content = in.readBytes(in.readInt()).array();
            return new RetrieveContentReplyMessage(requestId, name, content);
        }
    };
}




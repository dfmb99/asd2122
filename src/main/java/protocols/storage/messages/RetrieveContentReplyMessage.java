package protocols.storage.messages;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import protocols.storage.StorageProtocol;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.UUID;

public class RetrieveContentReplyMessage extends ProtoMessage {

    public final static short MSG_ID = 401;

    private final UUID requestId;
    private final String name;
    private final boolean notFound;
    private final byte[] content;

    public RetrieveContentReplyMessage(UUID requestId, String name, boolean notFound, byte[] content) {
        super(MSG_ID);
        this.requestId = requestId;
        this.name = name;
        this.notFound = notFound;
        this.content = content;
    }

    public UUID getRequestId() {
        return requestId;
    }

    public String getName() {
        return name;
    }

    public boolean isNotFound() {
        return notFound;
    }

    public byte[] getContent() {
        return content;
    }

    @Override
    public String toString() {
        return "RetrieveContentReplyMessage{" +
                "requestId=" + requestId +
                ", name='" + name + '\'' +
                ", notFound=" + notFound +
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
            out.writeBoolean(sampleMessage.notFound);
            if(!sampleMessage.notFound) {
                out.writeInt(sampleMessage.content.length);
                out.writeBytes(sampleMessage.content);
            }
            StorageProtocol.logger.info("Message sent with size {}", out.readableBytes());
        }

        @Override
        public RetrieveContentReplyMessage deserialize(ByteBuf in) {
            StorageProtocol.logger.info("Message received with size {}", in.readableBytes());
            UUID requestId = new UUID(in.readLong(), in.readLong());
            byte[] h = new byte[in.readInt()];
            in.readBytes(h);
            String name = new String(h, StandardCharsets.ISO_8859_1);
            boolean bNotFound = in.readBoolean();
            if(!bNotFound) {
                byte[] content = new byte[in.readInt()];
                in.readBytes(content);
                return new RetrieveContentReplyMessage(requestId, name, false, content);
            }
            else {
                return new RetrieveContentReplyMessage(requestId, name, true, null);
            }
        }
    };
}




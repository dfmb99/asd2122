package protocols.storage.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

public class StoreContentMessage extends ProtoMessage {

    public final static short MSG_ID = 402;

    private final UUID requestId;
    private final String name;
    private final byte[] content;

    public StoreContentMessage(UUID requestId, String name, byte[] content) {
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
        return "StoreContentMessage{" +
                "name=" + name +
                '}';
    }

    public static ISerializer<StoreContentMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(StoreContentMessage sampleMessage, ByteBuf out) {
            out.writeLong(sampleMessage.requestId.getMostSignificantBits());
            out.writeLong(sampleMessage.requestId.getLeastSignificantBits());
            byte[] nameBytes = StandardCharsets.ISO_8859_1.encode(sampleMessage.name).array();
            out.writeInt(nameBytes.length);
            out.writeBytes(nameBytes);
            out.writeInt(sampleMessage.content.length);
            out.writeBytes(sampleMessage.content);
        }

        @Override
        public StoreContentMessage deserialize(ByteBuf in) {
            UUID requestId = new UUID(in.readLong(), in.readLong());
            String name = new String(in.readBytes(in.readInt()).array(), StandardCharsets.ISO_8859_1);
            byte[] content = in.readBytes(in.readInt()).array();
            return new StoreContentMessage(requestId, name, content);
        }
    };
}




package protocols.storage.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

public class StoreContentReplyMessage extends ProtoMessage {

    public final static short MSG_ID = 403;

    private final UUID requestId;
    private final String name;

    public StoreContentReplyMessage(UUID requestId, String name) {
        super(MSG_ID);
        this.requestId = requestId;
        this.name = name;
    }

    public UUID getRequestId() {
        return requestId;
    }

    public String getName() {
        return name;
    }

    @Override
    public String toString() {
        return "StoreContentMessage{" +
                "name=" + name +
                '}';
    }

    public static ISerializer<StoreContentReplyMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(StoreContentReplyMessage sampleMessage, ByteBuf out) {
            out.writeLong(sampleMessage.requestId.getMostSignificantBits());
            out.writeLong(sampleMessage.requestId.getLeastSignificantBits());
            byte[] nameBytes = StandardCharsets.ISO_8859_1.encode(sampleMessage.name).array();
            out.writeInt(nameBytes.length);
            out.writeBytes(nameBytes);
        }

        @Override
        public StoreContentReplyMessage deserialize(ByteBuf in) {
            UUID requestId = new UUID(in.readLong(), in.readLong());
            String name = new String(in.readBytes(in.readInt()).array(), StandardCharsets.ISO_8859_1);
            return new StoreContentReplyMessage(requestId, name);
        }
    };
}




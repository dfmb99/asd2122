package protocols.storage.messages;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import protocols.dht.chord.ChordProtocol;
import protocols.storage.StorageProtocol;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

public class RetrieveContentMessage extends ProtoMessage {

    public final static short MSG_ID = 400;

    private final UUID requestId;
    private final String name;

    public RetrieveContentMessage(UUID requestId, String name) {
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
        return "RetrieveContentMessage{" +
                "requestId=" + requestId +
                "name=" + name +
                '}';
    }

    public static ISerializer<RetrieveContentMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(RetrieveContentMessage sampleMessage, ByteBuf out) {
            out.writeLong(sampleMessage.requestId.getMostSignificantBits());
            out.writeLong(sampleMessage.requestId.getLeastSignificantBits());
            byte[] nameBytes = StandardCharsets.ISO_8859_1.encode(sampleMessage.name).array();
            out.writeInt(nameBytes.length);
            out.writeBytes(nameBytes);
            StorageProtocol.logger.info("Message sent with size {}", out.readableBytes());
        }

        @Override
        public RetrieveContentMessage deserialize(ByteBuf in) {
            StorageProtocol.logger.info("Message received with size {}", in.readableBytes());
            UUID requestId = new UUID(in.readLong(), in.readLong());
            String name = new String(ByteBufUtil.getBytes(in.readBytes(in.readInt())), StandardCharsets.ISO_8859_1);
            return new RetrieveContentMessage(requestId, name);
        }
    };
}




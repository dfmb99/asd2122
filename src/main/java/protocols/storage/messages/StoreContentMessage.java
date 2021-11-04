package protocols.storage.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

public class StoreContentMessage extends ProtoMessage {

    public final static short MSG_ID = 101;

    private final String name;
    private final byte[] content;

    public StoreContentMessage(String name, byte[] content) {
        super(MSG_ID);
        this.name = name;
        this.content = content;
    }

    @Override
    public String toString() {
        return "StoreContentMessage{" +
                "name=" + name +
                '}';
    }

    public static ISerializer<StoreContentMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(StoreContentMessage sampleMessage, ByteBuf out) throws IOException {
            byte[] nameBytes = StandardCharsets.ISO_8859_1.encode(sampleMessage.name).array();
            out.writeInt(nameBytes.length);
            out.writeBytes(nameBytes);
            out.writeInt(sampleMessage.content.length);
            out.writeBytes(sampleMessage.content);
        }

        @Override
        public StoreContentMessage deserialize(ByteBuf in) throws IOException {
            String name = new String(in.readBytes(in.readInt()).array(), StandardCharsets.ISO_8859_1);
            byte[] content = in.readBytes(in.readInt()).array();
            return new StoreContentMessage(name, content);
        }
    };
}




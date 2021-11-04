package protocols.storage.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

public class RetrieveContentMessage extends ProtoMessage {

    public final static short MSG_ID = 101;

    private final String name;

    public RetrieveContentMessage(String name) {
        super(MSG_ID);
        this.name = name;
    }

    @Override
    public String toString() {
        return "RetrieveContentMessage{" +
                "name=" + name +
                '}';
    }

    public static ISerializer<RetrieveContentMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(RetrieveContentMessage sampleMessage, ByteBuf out) throws IOException {
            byte[] nameBytes = StandardCharsets.ISO_8859_1.encode(sampleMessage.name).array();
            out.writeInt(nameBytes.length);
            out.writeBytes(nameBytes);
        }

        @Override
        public RetrieveContentMessage deserialize(ByteBuf in) throws IOException {
            String name = new String(in.readBytes(in.readInt()).array(), StandardCharsets.ISO_8859_1);
            return new RetrieveContentMessage(name);
        }
    };
}




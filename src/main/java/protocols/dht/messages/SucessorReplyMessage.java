package protocols.dht.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class SucessorReplyMessage extends ProtoMessage {

    public final static short MSG_ID = 104;

    private final int id;
    private final byte[] contents;

    public SucessorReplyMessage(int id, byte[] contents) {
        super(MSG_ID);
        this.id = id;
        this.contents = contents;
    }

    public int getNodeId() {
        return this.id;
    }

    public byte[] getContents() {
        return this.contents;
    }

    @Override
    public String toString() {
        return "GetSucessorMessage{" +
                "id=" + id +
                "contents=" + new String(this.contents, StandardCharsets.UTF_8) +
                '}';
    }

    public static ISerializer<SucessorMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(SucessorMessage sampleMessage, ByteBuf out) throws IOException {
            // TODO: implement
        }

        @Override
        public SucessorMessage deserialize(ByteBuf in) throws IOException {
            // TODO: implement
            return null;
        }
    };
}




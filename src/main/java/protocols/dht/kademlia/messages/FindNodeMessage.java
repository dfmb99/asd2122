package protocols.dht.kademlia.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.io.IOException;
import java.math.BigInteger;

public class FindNodeMessage extends ProtoMessage {

    public final static short MSG_ID = 400;

    BigInteger id;
    public FindNodeMessage(BigInteger node) {
        super(MSG_ID);
        this.id = node;
    }


    public String toString() {
        return "FindNodeMessage{" +
                "id=" + id +
                '}';
    }

    public static ISerializer<FindNodeMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(FindNodeMessage sampleMessage, ByteBuf out) throws IOException {
            byte[] keyBytes = sampleMessage.id.toByteArray();
            out.writeInt(keyBytes.length);
            out.writeBytes(keyBytes);
        }

        @Override
        public FindNodeMessage deserialize(ByteBuf in) throws IOException {
            BigInteger id = new BigInteger(in.readBytes(in.readInt()).array());
            return new FindNodeMessage(id);
        }
    };
}

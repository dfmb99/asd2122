package protocols.dht.kademlia.messages;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.io.IOException;
import java.math.BigInteger;

public class FindNodeMessage extends ProtoMessage {

    public final static short MSG_ID = 400;

    private BigInteger id;
    private boolean isBootstrapping;

    public FindNodeMessage(BigInteger node, boolean isBootstrapping) {
        super(MSG_ID);
        this.id = node;
        this.isBootstrapping = isBootstrapping;
    }

    public BigInteger getLookUpId(){
        return id;
    }

    public boolean isBootstrapping() {
        return isBootstrapping;
    }

    public String toString() {
        return "FindNodeMessage{" +
                "id=" + id +
                '}';
    }

    public static ISerializer<FindNodeMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(FindNodeMessage sampleMessage, ByteBuf out) {
            byte[] keyBytes = sampleMessage.id.toByteArray();
            out.writeInt(keyBytes.length);
            out.writeBytes(keyBytes);
            out.writeBoolean(sampleMessage.isBootstrapping);
        }

        @Override
        public FindNodeMessage deserialize(ByteBuf in) {
            BigInteger id = new BigInteger(ByteBufUtil.getBytes(in.readBytes(in.readInt())));
            boolean bootstrapping = in.readBoolean();
            return new FindNodeMessage(id, bootstrapping);
        }
    };
}

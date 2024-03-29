package protocols.dht.kademlia.messages;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import protocols.dht.chord.ChordProtocol;
import protocols.dht.kademlia.KademliaProtocol;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.io.IOException;
import java.math.BigInteger;

public class FindNodeMessage extends ProtoMessage {

    public final static short MSG_ID = 300;

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
            KademliaProtocol.logger.info("Message sent with size {}", out.readableBytes());
        }

        @Override
        public FindNodeMessage deserialize(ByteBuf in) {
            KademliaProtocol.logger.info("Message received with size {}", in.readableBytes());
            byte[] h = new byte[in.readInt()];
            in.readBytes(h);
            BigInteger id = new BigInteger(h);
            boolean bootstrapping = in.readBoolean();
            return new FindNodeMessage(id, bootstrapping);
        }
    };
}

package protocols.dht.kademlia.messages;

import io.netty.buffer.ByteBuf;
import protocols.dht.kademlia.KademliaProtocol;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.util.UUID;

public class PingMessage extends ProtoMessage {

    public final static short MSG_ID = 302;

    UUID uid;

    public PingMessage(UUID uid) {
        super(MSG_ID);
        this.uid = uid;
    }

    public UUID getUid(){
        return uid;
    }

    @Override
    public String toString() {
        return "PingMessage";
    }

    public static ISerializer<PingMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(PingMessage sampleMessage, ByteBuf out) {
            out.writeLong(sampleMessage.uid.getMostSignificantBits());
            out.writeLong(sampleMessage.uid.getLeastSignificantBits());
            KademliaProtocol.logger.info("Message sent with size {}", out.readableBytes());
        }

        @Override
        public PingMessage deserialize(ByteBuf in) {
            KademliaProtocol.logger.info("Message received with size {}", in.readableBytes());
            UUID uid = new UUID(in.readLong(), in.readLong());
            return new PingMessage(uid);
        }
    };
}

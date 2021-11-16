package protocols.dht.kademlia.messages;

import io.netty.buffer.ByteBuf;
import protocols.dht.kademlia.KademliaProtocol;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.util.UUID;

public class PingReplyMessage extends ProtoMessage {

    public final static short MSG_ID = 303;

    UUID uid;

    public PingReplyMessage(UUID uid) {
        super(MSG_ID);
        this.uid = uid;
    }

    public UUID getUid(){
        return uid;
    }

    @Override
    public String toString() {
        return "PingReplyMessage";
    }

    public static ISerializer<PingReplyMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(PingReplyMessage sampleMessage, ByteBuf out) {
            out.writeLong(sampleMessage.uid.getMostSignificantBits());
            out.writeLong(sampleMessage.uid.getLeastSignificantBits());
            KademliaProtocol.logger.info("Message sent with size {}", out.readableBytes());
        }

        @Override
        public PingReplyMessage deserialize(ByteBuf in) {
            KademliaProtocol.logger.info("Message received with size {}", in.readableBytes());
            UUID uid = new UUID(in.readLong(), in.readLong());
            return new PingReplyMessage(uid);
        }
    };

}

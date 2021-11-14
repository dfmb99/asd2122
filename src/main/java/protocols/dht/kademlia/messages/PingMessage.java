package protocols.dht.kademlia.messages;

import io.netty.buffer.ByteBuf;
import protocols.dht.chord.messages.overlay.KeepAliveMessage;
import protocols.dht.kademlia.KademliaProtocol;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

public class PingMessage extends ProtoMessage {

    public final static short MSG_ID = 302;

    Double uid;

    public PingMessage(Double uid) {
        super(MSG_ID);
        this.uid = uid;
    }

    public Double getUid(){
        return uid;
    }

    @Override
    public String toString() {
        return "PingMessage";
    }

    public static ISerializer<PingMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(PingMessage sampleMessage, ByteBuf out) {
            out.writeDouble(sampleMessage.getUid());
            KademliaProtocol.logger.info("Message sent with size {}", out.readableBytes());
        }

        @Override
        public PingMessage deserialize(ByteBuf in) {
            KademliaProtocol.logger.info("Message received with size {}", in.readableBytes());
            Double uid = in.readDouble();
            return new PingMessage(uid);
        }
    };
}

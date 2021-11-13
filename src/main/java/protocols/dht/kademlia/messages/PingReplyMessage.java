package protocols.dht.kademlia.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

public class PingReplyMessage extends ProtoMessage {

    public final static short MSG_ID = 303;

    Double uid;

    public PingReplyMessage(Double uid) {
        super(MSG_ID);
        this.uid = uid;
    }

    public Double getUid(){
        return uid;
    }

    @Override
    public String toString() {
        return "PingReplyMessage";
    }

    public static ISerializer<PingReplyMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(PingReplyMessage sampleMessage, ByteBuf out) {
            out.writeDouble(sampleMessage.getUid());
        }

        @Override
        public PingReplyMessage deserialize(ByteBuf in) {
            Double uid = in.readDouble();
            return new PingReplyMessage(uid);
        }
    };

}

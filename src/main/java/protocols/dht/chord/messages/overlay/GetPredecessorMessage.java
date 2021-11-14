package protocols.dht.chord.messages.overlay;

import io.netty.buffer.ByteBuf;
import protocols.dht.chord.ChordProtocol;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

public class GetPredecessorMessage extends ProtoMessage {

    public final static short MSG_ID = 200;

    public GetPredecessorMessage() {
        super(MSG_ID);
    }

    @Override
    public String toString() {
        return "GetPredecessorMessage";
    }

    public static ISerializer<GetPredecessorMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(GetPredecessorMessage sampleMessage, ByteBuf out) {
            ChordProtocol.logger.info("Message sent with size {}", out.readableBytes());
        }

        @Override
        public GetPredecessorMessage deserialize(ByteBuf in) {
            ChordProtocol.logger.info("Message received with size {}", in.readableBytes());
            return new GetPredecessorMessage();
        }
    };
}

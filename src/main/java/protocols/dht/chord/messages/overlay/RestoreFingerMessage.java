package protocols.dht.chord.messages.overlay;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import protocols.dht.chord.ChordProtocol;
import protocols.dht.chord.types.ChordSegment;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.math.BigInteger;

public class RestoreFingerMessage extends ProtoMessage {

    public final static short MSG_ID = 206;

    private final ChordSegment segment;
    private final Host host;

    public RestoreFingerMessage(ChordSegment segment, Host host) {
        super(MSG_ID);
        this.segment = segment;
        this.host = host;
    }

    public ChordSegment getSegment() {
        return segment;
    }

    public Host getHost() {
        return host;
    }

    @Override
    public String toString() {
        return "RestoreFingerMessage{" +
                "segment=" + segment +
                ", host=" + host +
                '}';
    }

    public static ISerializer<RestoreFingerMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(RestoreFingerMessage sampleMessage, ByteBuf out) throws IOException {
            ChordSegment.serializer.serialize(sampleMessage.segment,out);
            Host.serializer.serialize(sampleMessage.host, out);
            ChordProtocol.logger.info("Message sent with size {}", out.readableBytes());
        }

        @Override
        public RestoreFingerMessage deserialize(ByteBuf in) throws IOException {
            ChordProtocol.logger.info("Message received with size {}", in.readableBytes());
            ChordSegment segment = ChordSegment.serializer.deserialize(in);
            Host host = Host.serializer.deserialize(in);
            return new RestoreFingerMessage(segment, host);
        }
    };
}




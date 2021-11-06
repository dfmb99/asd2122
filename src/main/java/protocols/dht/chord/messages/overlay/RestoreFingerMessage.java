package protocols.dht.chord.messages.overlay;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.math.BigInteger;

public class RestoreFingerMessage extends ProtoMessage {

    public final static short MSG_ID = 206;

    private final int finger;
    private final BigInteger key;
    private final Host host;

    public RestoreFingerMessage(int finger, BigInteger key, Host host) {
        super(MSG_ID);
        this.finger = finger;
        this.key = key;
        this.host = host;
    }

    public int getFinger() {
        return finger;
    }

    public BigInteger getKey() {
        return key;
    }

    public Host getHost() {
        return host;
    }

    @Override
    public String toString() {
        return "RestoreFingerMessage{" +
                "finger=" + finger +
                "key=" + key.toString() +
                "host=" + host.toString() +
                '}';
    }

    public static ISerializer<RestoreFingerMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(RestoreFingerMessage sampleMessage, ByteBuf out) throws IOException {
            out.writeInt(sampleMessage.finger);
            byte[] keyBytes = sampleMessage.key.toByteArray();
            out.writeInt(keyBytes.length);
            out.writeBytes(keyBytes);
            Host.serializer.serialize(sampleMessage.host, out);
        }

        @Override
        public RestoreFingerMessage deserialize(ByteBuf in) throws IOException {
            int finger = in.readInt();
            BigInteger key = new BigInteger(ByteBufUtil.getBytes(in.readBytes(in.readInt())));
            Host host = Host.serializer.deserialize(in);
            return new RestoreFingerMessage(finger, key, host);
        }
    };
}




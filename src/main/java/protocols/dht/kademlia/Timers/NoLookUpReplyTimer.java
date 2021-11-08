package protocols.dht.kademlia.Timers;

import protocols.dht.kademlia.types.Node;
import pt.unl.fct.di.novasys.babel.generic.ProtoTimer;

import java.math.BigInteger;

public class NoLookUpReplyTimer extends ProtoTimer {

    public static final short TIMER_ID = 300;

    private final BigInteger id;
    private final Node recipient;

    public NoLookUpReplyTimer(BigInteger id, Node recipient) {
        super(TIMER_ID);
        this.id = id;
        this.recipient = recipient;
    }

    @Override
    public ProtoTimer clone() {
        return this;
    }

    public BigInteger getLookUpId() {
        return id;
    }

    public Node getRecipient() {
        return recipient;
    }
}

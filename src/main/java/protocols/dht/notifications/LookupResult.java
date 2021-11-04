package protocols.dht.notifications;

import protocols.dht.chord.types.Node;
import pt.unl.fct.di.novasys.babel.generic.ProtoNotification;

import java.math.BigInteger;

public class LookupResult extends ProtoNotification {

    public static final short NOTIFICATION_ID = 116;

    private final BigInteger key;
    private final Node node;

    public LookupResult(BigInteger key, Node node) {
        super(NOTIFICATION_ID);
        this.key = key;
        this.node = node;
    }

    public BigInteger getKey() {
        return key;
    }

    public Node getNode() {
        return node;
    }
}

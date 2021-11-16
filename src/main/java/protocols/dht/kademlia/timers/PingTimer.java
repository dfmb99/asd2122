package protocols.dht.kademlia.timers;

import pt.unl.fct.di.novasys.babel.generic.ProtoTimer;

import java.util.UUID;

public class PingTimer extends ProtoTimer {

    public static final short TIMER_ID = 400;

    private UUID pingUid;

    public PingTimer(UUID pingUid) {
        super(TIMER_ID);
        this.pingUid = pingUid;
    }

    public UUID getPingUid() {
        return pingUid;
    }

    public String toString(){
        return "PingTimedOut{ uid:" + pingUid + " }";
    }

    @Override
    public ProtoTimer clone() {
        return this;
    }
}

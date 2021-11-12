package protocols.dht.kademlia.timers;

import pt.unl.fct.di.novasys.babel.generic.ProtoTimer;

public class PingTimer extends ProtoTimer {

    public static final short TIMER_ID = 400;

    private Double pingUid;

    public PingTimer(Double pingUid) {
        super(TIMER_ID);
        this.pingUid = pingUid;
    }

    public Double getPingUid() {
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

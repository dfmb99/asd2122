package protocols.dht.kademlia.timers;

import pt.unl.fct.di.novasys.babel.generic.ProtoTimer;

public class PingReplyTimer extends ProtoTimer {

    public PingReplyTimer(short id) {
        super(id);
    }

    @Override
    public ProtoTimer clone() {
        return this;
    }
}

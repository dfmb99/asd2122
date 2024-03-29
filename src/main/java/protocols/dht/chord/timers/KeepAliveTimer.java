package protocols.dht.chord.timers;

import pt.unl.fct.di.novasys.babel.generic.ProtoTimer;

public class KeepAliveTimer extends ProtoTimer {

    public static final short TIMER_ID = 202;

    public KeepAliveTimer() {
        super(TIMER_ID);
    }

    @Override
    public ProtoTimer clone() {
        return this;
    }
}

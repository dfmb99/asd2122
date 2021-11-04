package protocols.dht.notifications;

import pt.unl.fct.di.novasys.babel.generic.ProtoNotification;

public class ChannelCreated extends ProtoNotification {

    public static final short NOTIFICATION_ID = 115;

    private final int channelId;

    public ChannelCreated(int channelId) {
        super(NOTIFICATION_ID);
        this.channelId = channelId;
    }

    public int getChannelId() {
        return channelId;
    }
}
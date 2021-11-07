package notifications;

import protocols.Channel;
import pt.unl.fct.di.novasys.babel.generic.ProtoNotification;

public class ChannelCreated extends ProtoNotification {

    public static final short NOTIFICATION_ID = 100;

    public final Channel channel;

    public ChannelCreated(Channel channel) {
        super(NOTIFICATION_ID);
        this.channel = channel;
    }
}

package protocols.apps;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import notifications.ChannelCreated;
import protocols.apps.timers.ExitTimer;
import protocols.apps.timers.RequestTimer;
import protocols.apps.timers.StartTimer;
import protocols.apps.timers.StopTimer;
import protocols.storage.replies.RetrieveFailedReply;
import protocols.storage.replies.RetrieveOKReply;
import protocols.storage.replies.StoreOKReply;
import protocols.storage.requests.RetrieveRequest;
import protocols.storage.requests.StoreRequest;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.channel.tcp.events.ChannelMetrics;
import pt.unl.fct.di.novasys.network.data.Host;

public class AutomatedApplication extends GenericProtocol {
	private static final Logger logger = LogManager.getLogger(AutomatedApplication.class);

	//Protocol information, to register in babel
	public static final String PROTO_NAME = "AutomatedApplication";
	public static final short PROTO_ID = 10;

	private final short storageProtoId;

	//Number of different contents to be stored in the DHT by this process
	private final int numberContents;
	//Size of the payload for contents stored (in bytes)
	private final int payloadSize;
	//Time to wait until starting storing messages
	private final int storeTime;
	//Time to wait until starting retrieving messages
	private final int retrieveTime;
	//Time to run before shutting down
	private final int runTime;
	//Time to wait until starting sending messages
	private final int cooldownTime;
	//Interval between each request
	private final int requestInterval;

	//Number of total processes in the system (for testing purposes)
	private final int totalProcesses;
	//Index of this process (for testing purposes)
	private final int localIndex;

	private final Host self;

	private long requestTimer;

	private boolean bStartedSendingRetrieveRequests;

	//Variables related with the Workload
	private Random r;
	private List<String> myKeys;
	private List<String> otherKeys;
	private int storedKeys;
	
	//Variables related with measurement
	private long storeRequests = 0;
	private long storeRequestsCompleted = 0;
	private long retrieveRequests = 0;
	private long retrieveRequestsSuccessful = 0;
	private long retrieveRequestsFailed = 0;
	
	public AutomatedApplication(Host self, Properties properties, short storageProtoId) throws HandlerRegistrationException {
		super(PROTO_NAME, PROTO_ID);
		this.storageProtoId = storageProtoId;
		this.self = self;

		//Read configurations
		this.numberContents = Integer.parseInt(properties.getProperty("content_number"), 20);
		this.payloadSize = Integer.parseInt(properties.getProperty("payload_size"));
		this.storeTime = Integer.parseInt(properties.getProperty("store_time")); //in seconds
		this.retrieveTime = Integer.parseInt(properties.getProperty("retrieve_time")); //in seconds
		this.cooldownTime = Integer.parseInt(properties.getProperty("cooldown_time")); //in seconds
		this.runTime = Integer.parseInt(properties.getProperty("run_time")); //in seconds
		this.requestInterval = Integer.parseInt(properties.getProperty("request_interval")); //in milliseconds

		this.bStartedSendingRetrieveRequests = false;

		//Setup handlers
		registerTimerHandler(RequestTimer.TIMER_ID, this::uponRequestTimer);
		registerTimerHandler(StartTimer.TIMER_ID, this::uponStartTimer);
		registerTimerHandler(StopTimer.TIMER_ID, this::uponStopTimer);
		registerTimerHandler(ExitTimer.TIMER_ID, this::uponExitTimer);
		registerReplyHandler(StoreOKReply.REPLY_TYPE_ID, this::uponStoreOk);
		registerReplyHandler(RetrieveOKReply.REPLY_TYPE_ID, this::uponRetrieveOK);
		registerReplyHandler(RetrieveFailedReply.REPLY_TYPE_ID, this::uponRetrieveFailed);

		//Register notifications
		subscribeNotification(ChannelCreated.NOTIFICATION_ID, this::uponChannelCreated);

		//Variables related with workload generation
		this.totalProcesses = Integer.parseInt(properties.getProperty("total_processes"));
		this.localIndex = Integer.parseInt(properties.getProperty("my_index"));
		this.myKeys = new ArrayList<>(this.numberContents);
		this.otherKeys = new ArrayList<>(this.numberContents * (this.totalProcesses-1));
		this.storedKeys = 0;
	}

	private void uponChannelCreated(ChannelCreated notification, short sourceProto) {
		try {
			registerSharedChannel(notification.channel.id);
			registerChannelEventHandler(notification.channel.id, ChannelMetrics.EVENT_ID, this::uponChannelMetrics);
		} catch (HandlerRegistrationException e) {
			e.printStackTrace();
		}
	}
	
	
	@Override
	public void init(Properties props) {
		//Generate Keys deterministically;
		logger.debug("Generating Keys");
		logger.info("local index: {} total_processes: {}", this.localIndex, this.totalProcesses);
		for(int i = 1; i <= this.totalProcesses; i++) {
			r = new Random(i);
			for(int j = 0; j < numberContents; j++) {
				String key = "content_" + i + "_" + new BigInteger(256, 0, r);
				if(i == this.localIndex) {
					this.myKeys.add(key);
				} else {
					this.otherKeys.add(key);
				}
			}
		}	
		//reset Random
		r = new Random(this.localIndex);
		//Wait prepareTime seconds before starting
		logger.debug("Waiting...");
		setupTimer(new StartTimer(), storeTime * 1000L);
	}

	private void uponStartTimer(StartTimer startTimer, long timerId) {
		logger.info("Starting");
		byte[] content = new byte[this.payloadSize];
		new Random(this.localIndex* 1000L + this.storedKeys).nextBytes(content);
		StoreRequest request = new StoreRequest(this.myKeys.get(this.storedKeys), content);
		sendRequest(request, storageProtoId);
		logger.info("Sent request {}", request);
		this.storeRequests++;
	}

	private void uponRequestTimer(RequestTimer broadcastTimer, long timerId) {
		String name = this.otherKeys.get(r.nextInt(this.otherKeys.size()));
		
		RetrieveRequest request = new RetrieveRequest(name);
		logger.info("Sent request {}", request);
		//And send it to the storage protocol
		sendRequest(request, storageProtoId);
		this.retrieveRequests++;
	}

	private void uponStoreOk(StoreOKReply reply, short sourceProto) {
		logger.info("Received reply {}", reply);
		this.storedKeys++;
		this.storeRequestsCompleted++;
		if(this.storedKeys >= this.numberContents) {
			if(bStartedSendingRetrieveRequests) return;

			bStartedSendingRetrieveRequests = true;
			//Start requests periodically
			requestTimer = setupPeriodicTimer(new RequestTimer(), retrieveTime * 1000L, requestInterval);
			//And setup the stop timer
			setupTimer(new StopTimer(), (retrieveTime + runTime)*1000L);
		} else {
			byte[] content = new byte[this.payloadSize];
			new Random(this.localIndex* 1000L +this.storedKeys).nextBytes(content);
			StoreRequest request = new StoreRequest(this.myKeys.get(this.storedKeys), content);
			logger.info("Sent request {}", request);
			sendRequest(request, storageProtoId);
			this.storeRequests++;
		}
	}
	
	private void uponRetrieveOK(RetrieveOKReply reply, short sourceProto) {
		logger.info("Received reply {}", reply);
		this.retrieveRequestsSuccessful++;
	}
	
	private void uponRetrieveFailed(RetrieveFailedReply reply, short sourceProto) {
		logger.info("Received reply {}", reply);
		this.retrieveRequestsFailed++;
	}

	private void uponStopTimer(StopTimer stopTimer, long timerId) {
		logger.info("Stopping broadcasts");
		this.cancelTimer(requestTimer);
		setupTimer(new ExitTimer(), cooldownTime * 1000L);
	}

	private void uponExitTimer(ExitTimer exitTimer, long timerId) {
		logger.debug("Exiting...");
		logger.info("{}: Executed {} store requests.", self, this.storeRequests);
		logger.info("{}: Completed {} store requests.", self, this.storeRequestsCompleted);
		logger.info("{}: Executed {} retrieve requests.", self, this.retrieveRequests);
		logger.info("{}: Success on {} retrieve requests.", self, this.retrieveRequestsSuccessful);
		logger.info("{}: Failed on {} retrieve requests.", self, this.retrieveRequestsFailed);
		System.exit(0);
	}
	
	//If we passed a value >0 in the METRICS_INTERVAL_KEY property of the channel, this event will be triggered
    //periodically by the channel. This is NOT a protocol timer, but a channel event.
    //Again, we are just showing some of the information you can get from the channel, and use how you see fit.
    //"getInConnections" and "getOutConnections" returns the currently established connection to/from me.
    //"getOldInConnections" and "getOldOutConnections" returns connections that have already been closed.
    private void uponChannelMetrics(ChannelMetrics event, int channelId) {
        StringBuilder sb = new StringBuilder("Channel Metrics:\n");
        sb.append("In channels:\n");
        event.getInConnections().forEach(c -> sb.append(String.format("\t%s: msgOut=%s (%s) msgIn=%s (%s)\n",
                c.getPeer(), c.getSentAppMessages(), c.getSentAppBytes(), c.getReceivedAppMessages(),
                c.getReceivedAppBytes())));
        event.getOldInConnections().forEach(c -> sb.append(String.format("\t%s: msgOut=%s (%s) msgIn=%s (%s) (old)\n",
                c.getPeer(), c.getSentAppMessages(), c.getSentAppBytes(), c.getReceivedAppMessages(),
                c.getReceivedAppBytes())));
        sb.append("Out channels:\n");
        event.getOutConnections().forEach(c -> sb.append(String.format("\t%s: msgOut=%s (%s) msgIn=%s (%s)\n",
                c.getPeer(), c.getSentAppMessages(), c.getSentAppBytes(), c.getReceivedAppMessages(),
                c.getReceivedAppBytes())));
        event.getOldOutConnections().forEach(c -> sb.append(String.format("\t%s: msgOut=%s (%s) msgIn=%s (%s) (old)\n",
                c.getPeer(), c.getSentAppMessages(), c.getSentAppBytes(), c.getReceivedAppMessages(),
                c.getReceivedAppBytes())));
        sb.setLength(sb.length() - 1);
        logger.info(sb);
    }
}

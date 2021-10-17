package protocols.dht.replies;

import java.math.BigInteger;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.UUID;

import pt.unl.fct.di.novasys.babel.generic.ProtoReply;
import pt.unl.fct.di.novasys.network.data.Host;

public class LookupRequest extends ProtoReply {

	public final static short REPLY_ID = 102;
	
	private BigInteger id;
	private UUID uid;
	private Set<Host> peers;
	
	public LookupRequest(BigInteger id, Collection<Host> peers, UUID uid) {
		super(REPLY_ID);
		this.id = id;
		this.uid = uid;
		this.peers = new HashSet<Host>(peers);
	}
	
	public LookupRequest(BigInteger id, Host peer, UUID uid) {
		super(REPLY_ID);
		this.id = id;
		this.uid = uid;
		this.peers = new HashSet<Host>();
		this.peers.add(peer);
	}
	
	public UUID getRequestUID() {
		return this.uid;
	}
	
	public BigInteger getID() {
		return this.id;
	}
	
	public int getNumberOfPeers() {
		return this.peers.size();
	}
	
	public Iterator<Host> getPeerIterator() {
		return this.peers.iterator();
	}
	
	public Host getFirstPeer() {
		if(this.peers.size() > 0) {
			return this.peers.iterator().next();
		} else {
			return null;
		}
	}


}

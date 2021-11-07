package protocols.dht.kademlia.types;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class Bucket {

    private List<Node> nodes;
    private int max;

    public Bucket(int max) {
        this.nodes = new ArrayList<>();
        this.max = max;
    }

    public void addNode(Node n) {
        this.nodes.add(n);
    }

    public void rmvNode(Node n) {
        this.nodes.remove(n);
    }

    public boolean containsNode(Node n) {
        return this.nodes.contains(n);
    }

    public Iterator<Node> getIterator() {
        return this.nodes.iterator();
    }

    public boolean isFull() {
        return this.nodes.size() >= max;
    }
}

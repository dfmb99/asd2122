package protocols.storage;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class VolatileStorage implements IPersistentStorage {

    Map<String, byte[]> storage;

    public VolatileStorage() {
        this.storage = new ConcurrentHashMap<>();
    }

    @Override
    public byte[] get(String key) {
        return storage.get(key);
    }

    @Override
    public void put(String key, byte[] content) {
        storage.put(key,content);
    }
}

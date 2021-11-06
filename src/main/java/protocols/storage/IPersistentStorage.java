package protocols.storage;

public interface IPersistentStorage {
    byte[] get(String key);
    void put(String key, byte[] content);
}

package tiger;

public interface KafkaMessageCallback {
    void onMessage(String key, String value);
}

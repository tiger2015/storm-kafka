package tiger.service;

/**
 * @Auther: Administrator
 * @Date: 2019/10/14 17:28
 * @Description:
 */
public interface KafkaProducerService<K, V> {

    void sendMessage(String topic, K key, V value);
}

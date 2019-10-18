package tiger.dao;

import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * @Auther: Administrator
 * @Date: 2019/10/14 17:24
 * @Description:
 */
public interface KafkaProducerDao<K, V> {

    void sendMessage(ProducerRecord<K, V> record);
}

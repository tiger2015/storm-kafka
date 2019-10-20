package tiger.service;


import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import tiger.dao.KafkaProducerDao;

/**
 * @ClassName KafkaProducerServiceImpl
 * @Description TODO
 * @Author zeng.h
 * @Date 2019/10/14 17:29
 * @Version 1.0
 **/
@Service
public class KafkaProducerServiceImpl<K, V> implements KafkaProducerService<K, V> {

    @Autowired
    private KafkaProducerDao<K, V> kafkaProducerDao;

    @Override
    public void sendMessage(String topic, K key, V value) {
        ProducerRecord<K, V> record = new ProducerRecord<>(topic, key, value);
        kafkaProducerDao.sendMessage(record);
    }
}

package tiger.dao;


import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Repository;

/**
 *@ClassName KafkaProducerDaoImpl
 *@Description TODO
 *@Author zeng.h
 *@Date 2019/10/14 17:26
 *@Version 1.0
 **/
@Repository
@Slf4j
public class KafkaProducerDaoImpl<K,V> implements KafkaProducerDao<K,V>{

    @Autowired
    private KafkaTemplate<K,V> kafkaTemplate;


    @Override
    public void sendMessage(ProducerRecord<K, V> record) {
        kafkaTemplate.send(record);

    }
}

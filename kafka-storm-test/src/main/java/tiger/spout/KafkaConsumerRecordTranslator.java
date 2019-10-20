package tiger.spout;


import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.storm.kafka.spout.RecordTranslator;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.List;

/**
 * @ClassName KafkaConsumerRecordTranslator
 * @Description TODO
 * @Author zeng.h
 * @Date 2019/10/15 11:28
 * @Version 1.0
 **/
@Slf4j
public class KafkaConsumerRecordTranslator<K, V> implements RecordTranslator<K, V> {

    @Override
    public List<Object> apply(ConsumerRecord<K, V> record) {
        return new Values(record.key(), record.value());
    }

    @Override
    public Fields getFieldsFor(String stream) {
        return new Fields("id", "message");
    }
}

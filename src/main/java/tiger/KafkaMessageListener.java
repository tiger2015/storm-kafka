package tiger;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.concurrent.LinkedBlockingQueue;

//@Component
@Slf4j
public class KafkaMessageListener {

   public static LinkedBlockingQueue<ConsumerRecord<String, String>> messageQueue = new LinkedBlockingQueue<>();

    //@KafkaListener(topics = "${kafka.consumer.topics}", containerFactory = "listenerContainerFactory")
    public void onBatchMessage(ConsumerRecords<String, String> records) {
        log.info("receive message count:" + records.count());
        for (ConsumerRecord<String, String> record : records) {
            try {
                messageQueue.put(record);
            } catch (InterruptedException e) {
                log.error("add message to queue error", e);
            }
        }
    }
}

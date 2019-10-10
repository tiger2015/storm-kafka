package tiger;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.BatchMessageListener;

import java.util.List;

/**
 * @ClassName MyBatchMessageListener
 * @Description TODO
 * @Author zeng.h
 * @Date 2019/10/10 16:46
 * @Version 1.0
 **/
public class MyBatchMessageListener implements BatchMessageListener<String, String> {

    @Override
    public void onMessage(List<ConsumerRecord<String, String>> data) {



    }
}

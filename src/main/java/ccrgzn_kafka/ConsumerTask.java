package ccrgzn_kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public class ConsumerTask implements Runnable {
    Properties props = null;
    AtomicBoolean isRunning = null;

    public ConsumerTask(Properties props, AtomicBoolean isRunning) {
        this.props = props;
        this.isRunning = isRunning;
    }

    public void run() {

        //2.构建consumer实例
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        //3.订阅主题
        consumer.subscribe(Arrays.asList("tpc_2"));

        //拉取消息
        while (isRunning.get()) {

            //每一次poll得到的结果中，可能包含多个topic的多个partition的消息
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(Long.MAX_VALUE));
            for (ConsumerRecord<String, String> record : records) {
                //do some process做一些处理
                System.out.println(record.key() + ","
                        + record.value() + ","
                        + record.topic() + ","
                        + record.partition() + ","
                        + record.offset());
                System.out.println("------------------------lst---------------------------");

            }
        }

        consumer.close(Duration.ofMillis(1000));
    }

}

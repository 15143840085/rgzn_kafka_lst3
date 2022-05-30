package ccrgzn_kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import java.time.Duration;
import java.util.Arrays;

import java.util.Properties;
import java.util.Set;

public class ConsumerSeekOffset {
    private static final String SERVERS = "192.168.88.151:9092,192.168.88.152:9092,192.168.88.153:9092";
    public static void main(String[] args) {

        //1.参数配置
        Properties props = new Properties();
        //key的反序列化器
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        //value的反序列化器
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        //服务器地址
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,SERVERS);
        //设置自动读取的起始offset（偏移量），值可以是：earliest，latest，none
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        //设置自动提交offset（偏移量）
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,true);
        //设置消费者组
        props.put(ConsumerConfig.GROUP_ID_CONFIG,"b1");
        //创建kafka实例对象
        KafkaConsumer<String,String> kafkaConsumer = new KafkaConsumer<>(props);
        //订阅主题
        kafkaConsumer.subscribe(Arrays.asList("tpc_2"));

//        kafkaConsumer.subscribe(Arrays.asList("tcp_2"), new ConsumerRebalanceListener() {
//            @Override
//            public void onPartitionsRevoked(Collection<TopicPartition> collection) {
//
//            }
//
//            @Override
//            public void onPartitionsAssigned(Collection<TopicPartition> collection) {
//
//            }
//        });

        //先拉取一次消息
        kafkaConsumer.poll(Duration.ofMillis(5000));
        // 先看看被分配了哪些topic中的分区
        Set<TopicPartition> assignment = kafkaConsumer.assignment();
        //对于被分配的分区，全部统一定位到offset=100的位置成为初始偏移量
        for (TopicPartition topicPartition : assignment){
            kafkaConsumer.seek(topicPartition,100);
        }

        while (true) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(Long.MAX_VALUE));
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
    }
}

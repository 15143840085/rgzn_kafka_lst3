package ccrgzn_kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class ConsumerDemo2 {
    private static final String SERVERS = "192.168.88.151:9092,192.168.88.152:9092,192.168.88.153:9092";
    public static void main(String[] args) {

        //1.参数配置
        Properties props = new Properties();
        //key的反序列化器
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        //value的反序列化器
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        //服务器地址
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVERS);
        //设置自动读取的起始offset（偏移量），值可以是：earliest，latest，none
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        //设置自动提交offset（偏移量）
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        //设置消费者组
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "g1");

        //2.创建consumer实例对象
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(props);
        //3.1构造相应的分区集合
        TopicPartition tpc_2_0 = new TopicPartition("tpc_2",0);
        TopicPartition tpc_2_1 = new TopicPartition("tpc_2",1);
        //3.2通过assign实现订阅
        kafkaConsumer.assign(Arrays.asList(tpc_2_0,tpc_2_1));

        while (true){
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(Long.MAX_VALUE));

            List<ConsumerRecord<String, String>> records1 = records.records(tpc_2_0);
            for (ConsumerRecord<String, String> rec : records1) {
                //do some process做一些处理
                System.out.println(rec.key()+","
                        +rec.value()+","
                        +rec.topic()+","
                        +rec.partition()+","
                        +rec.offset());
                System.out.println("------------------------lst---------------------------");

            }
            List<ConsumerRecord<String, String>> records2 = records.records(tpc_2_1);
            for (ConsumerRecord<String, String> rec : records2) {
                //do some process做一些处理
                System.out.println(rec.key()+","
                        +rec.value()+","
                        +rec.topic()+","
                        +rec.partition()+","
                        +rec.offset());
                System.out.println("------------------------lst---------------------------");

            }
        }





    }
}

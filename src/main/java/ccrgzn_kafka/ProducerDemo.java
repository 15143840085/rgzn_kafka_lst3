package ccrgzn_kafka;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;


import java.util.Properties;


public class ProducerDemo {
    public  static final String SERVERS = "192.168.88.151:9092,192.168.88.152:9092,192.168.88.153:9092";
    public static  void main(String[] args)   {
        /**
         * 配置生产者参数
         * @param args
         */
//        创建对象
        Properties props = new Properties();
        //配置方式1
//        props.load(ProducerDemo.class.getClassLoader().getResourceAsStream('client.properties'));
//        props.put('bootstrap.servers','node1:9092,node2:9092,node3:9092');
//        配置方式2，利用常量类，去进行配置，不容易写错参数名，比较容易记忆
//        设置kafka集群的地址
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,SERVERS);
//        ack模式，取值有0，1，-1（all），all是最慢最安全的
        props.put(ProducerConfig.ACKS_CONFIG,"all");
//        失败重试次数-->失败会自动重试（可恢复/不可恢复）-->有可能会造成数据的乱序
//        props.put(ProducerConfig.RETRIES_CONFIG,3);
////        数据发送的批次大小 提高效率/吞吐量太大数据会延迟
//        props.put(ProducerConfig.BATCH_SIZE_CONFIG,10);
////        消息在缓冲区保留的时间，超过设置的值就会被提交到服务端
//        props.put(ProducerConfig.LINGER_MS_CONFIG,10000);
////        数据发送请求的最大缓存数
//        props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG,10);
////        整个Producer用到的总内存的大小，如果缓冲区满了就会提交到服务端/buffer.memory 要大于batch.size否则会报申请内存不足的错误降低阻塞的可能性
//        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG,1024);
////        序列化器，字符串最好
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
//        关闭kafka幂等性的功能
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,"false");

        /**
         * 步骤2.创建相应的生产者实例
         * @param args
         */
        KafkaProducer<String,String> producer = new KafkaProducer<>(props);
        /**
         * 步骤3.构建待发送的消息
         *
         */
        for(int i = 0; i < 200; i++)
        {
            ProducerRecord<String, String> msg = new ProducerRecord<>("tpc_2", "name" + i, "bigdata19-2-lst+"+i );
            producer.send(msg);
        }

        /**
         * 步骤5.关闭
         */

        producer.close();

    }
}

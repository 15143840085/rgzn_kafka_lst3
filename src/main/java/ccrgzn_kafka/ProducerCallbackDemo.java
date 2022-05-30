package ccrgzn_kafka;


import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerCallbackDemo {
    public  static final String SERVERS = "192.168.88.151:9092,192.168.88.152:9092,192.168.88.153:9092";
    public static void main(String[] args) {
        /**
         * 配置生产者参数
         * @param args
         */
        Properties props = new Properties(-lst);
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        /**
         * 步骤2.创建相应的生产者实例
         * @param args
         */
        KafkaProducer<String,String> producer = new KafkaProducer<>(props);
        /**
         * 步骤3.构建待发送的消息
         *
         */
        for(int i = 0;i < 120;i++){
            ProducerRecord<String,String> rcd = new ProducerRecord<String,String>("tpc_1", "key" + i, "rgzn_bigdata_1902_lst+"+i);
            producer.send(rcd, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {

                if(recordMetadata !=null){
                    System.out.println(recordMetadata.topic());
                    System.out.println(recordMetadata.offset());
                    System.out.println(recordMetadata.serializedKeySize());
                    System.out.println(recordMetadata.serializedValueSize());
                    System.out.println(recordMetadata.timestamp());
                }
                }
            });
        }
        producer.close();
    }
}

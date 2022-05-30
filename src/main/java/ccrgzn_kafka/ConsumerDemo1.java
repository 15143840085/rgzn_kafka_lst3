package ccrgzn_kafka;


import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public class ConsumerDemo1 {
    private static final String SERVERS = "192.168.88.151:9092,192.168.88.152:9092,192.168.88.153:9092";

    public static void main(String[] args) throws InterruptedException {

        //定义一个AtomicBoolean类型的isRunning来控制消费者拉取消息
        AtomicBoolean isRunning = new AtomicBoolean(true);

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
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "b1");

        Thread thread = new Thread(new ConsumerTask(props,isRunning));

        thread.start();

        //主线程可以去休眠60s
        Thread.sleep(60000);



        //修改isRunning的值为false
        isRunning.set(false);
    }
}

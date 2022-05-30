package ccrgzn_kafka;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.common.KafkaFuture;

import java.util.Properties;
import java.util.Set;

public class kafkaAdminDemo {

    private static final String SERVERS = "192.168.88.151:9092,192.168.88.152:9092,192.168.88.153:9092";

    public static void main(String[] args) {

        Properties props = new Properties();

        props.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,SERVERS);

        //1.构造一个管理客户端的对象
        AdminClient adminClient = KafkaAdminClient.create(props);

        //2.列出集群中的主题信息
        ListTopicsResult listTopicsResult = adminClient.listTopics();
        KafkaFuture<Set<String>> names = listTopicsResult.names();


    }
}

package kafka;

import helpers.Helper;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.Collections;
import java.util.Properties;

public class TopicCreator {
    public static void  creatTopic(String topicName, int numPartitions) throws Exception{
        Properties config = new Properties();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, Helper.BROKERS);
        AdminClient admin = AdminClient.create(config);

        Boolean existsTopic = admin.listTopics().names().get().stream().anyMatch(existingTopicName -> existingTopicName.equals(topicName));

        if (existsTopic == Boolean.FALSE){
            NewTopic newTopic = new NewTopic(topicName, numPartitions, (short) 1);
            admin.createTopics(Collections.singleton(newTopic));
        }
    }

    private TopicCreator() {}

}

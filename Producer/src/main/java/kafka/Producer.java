package kafka;

import helpers.Helper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import entities.User;

import java.util.Properties;

public class Producer extends Thread {
    private final KafkaProducer<String, String> producer;
    public final String topic;

    public static final String KAFKA_SERVER_URL = Helper.BROKERS;
    public static final String CLIENT_ID = Helper.CLIENT_ID;

    public Producer(String topic, Integer numPartitions) throws  Exception{
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER_URL);
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, CLIENT_ID);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producer = new KafkaProducer<>(properties);
        TopicCreator.creatTopic(topic, numPartitions);
        this.topic = topic;

    }


    public void send(User user) {
        this.producer.send(new ProducerRecord<>(this.topic, user.getUsername(), user.getPassword()));
        this.producer.flush();
    }
    public void close() {
        this.producer.close();
    }
}

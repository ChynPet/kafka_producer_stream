package kafka;

import helpers.Helper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.SessionWindows;

import java.time.Duration;
import java.util.Properties;

public class Stream {
    private final Properties properties;
    private final StreamsBuilder streamsBuilder;
    private final KStream<String, String> source;

    private final SessionWindows sessionWindows;

    private KafkaStreams streams;

    public final String topicInput;
    public final String topicOutput;

    public static final String KAFKA_SERVER_URL = Helper.BROKERS;
    public static final String CLIENT_ID = Helper.APP_ID;
    public final Duration windowSizeMs;

    public Stream(String topicInput, String topicOutput, Integer numPartitions, Duration windowSizeMs) throws Exception {
        this.topicInput = topicInput;
        this.topicOutput = topicOutput;
        TopicCreator.creatTopic(topicInput, numPartitions);
        TopicCreator.creatTopic(topicOutput, numPartitions);

        this.properties = new Properties();
        this.properties.put(StreamsConfig.APPLICATION_ID_CONFIG, CLIENT_ID);
        this.properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER_URL);
        this.properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        this.properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());


        this.streamsBuilder = new StreamsBuilder();
        this.source = this.streamsBuilder.stream(topicInput);
        this.windowSizeMs = windowSizeMs;
        this.sessionWindows = SessionWindows.with(this.windowSizeMs);
    }

    public Stream initTopology(){
       KStream<String, String> topology = this.source.groupByKey()
                .windowedBy(this.sessionWindows)
                .count()
                .toStream()
                .map((key, value) -> new KeyValue<>(key.key(),(value >= 10) ? "1":"0"));

        topology.to(this.topicOutput);
        return this;
    }

    public Stream buildStreams(){
        this.streams = new KafkaStreams(this.streamsBuilder.build(), this.properties);
        return this;
    }

    public void start(){
        this.streams.cleanUp();
        this.streams.start();
    }

    public void close(){
        this.streams.close();
    }

}

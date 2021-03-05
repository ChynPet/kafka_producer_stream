# kafka_producer_stream
Realization of the Kafka Producer and Kafka Stream

# RUN
## Grant permission before execution
```bash
chmod 777 ./stream
chmod 777 ./producer
```
Before starting, run Kafka at **localhost:9092**

1. Run Kafka Stream with help command
```bash 
./stream	
```
2. In the new console tab, launch Kafka Producer
```bash
./producer
```
After launch, two topics "input" and "detect" will be created. The result of detecting brute force will be written in the topic "detect".
1 - there is brute force
0 - there is not brute force
# Class
This repository contains two projects. One is implemented by Kafka Streams, the other by Kafka Producer.
Kafka Stream
```java
public class Stream {
    private final Properties properties;
    private final StreamsBuilder streamsBuilder;
    private final KStream<String, String> source;

    private final SessionWindows sessionWindows;

    private KafkaStreams streams;

    public final String topicInput;
    public final String topicOutput;
    
    public Stream(String topicInput, String topicOutput, Integer numPartitions, Duration windowSizeMs);
    
    public static final String KAFKA_SERVER_URL = Helper.BROKERS;
    public static final String CLIENT_ID = Helper.APP_ID;
    public final Duration windowSizeMs;

    public Stream initTopology();

    public Stream buildStreams();

    public void start();
    public void close();
```
Kafka Producer
```java
    private final KafkaProducer<String, String> producer;
    public final String topic;

    public static final String KAFKA_SERVER_URL = Helper.BROKERS;
    public static final String CLIENT_ID = Helper.CLIENT_ID;

    public Producer(String topic, Integer numPartitions);

    public void send(User user);
    public void close();
```

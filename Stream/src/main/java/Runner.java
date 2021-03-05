import kafka.Stream;

import java.time.Duration;


public class Runner {
    public static void main(String[] args) throws Exception {
        Stream stream = new Stream("input", "detect", 1, Duration.ofMillis(1000));
        stream.initTopology().buildStreams().start();
    }
}

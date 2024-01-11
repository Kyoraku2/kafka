package demo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;


public final class LogsProcessing{
    public static final String INPUT_TOPIC = "raw-logs";
    public static final String OUTPUT_TOPIC = "quickstart-events";

    public static Properties getStreamsConfig(final String[] args) throws IOException {
        final Properties props = new Properties();
        if (args != null && args.length > 0) {
            try (final FileInputStream fis = new FileInputStream(args[0])) {
                props.load(fis);
            }
            if (args.length > 1) {
                System.out.println("Warning: Some command line arguments were ignored. This demo only accepts an optional configuration file.");
            }
        }
        props.putIfAbsent(StreamsConfig.APPLICATION_ID_CONFIG, "apache-log-analyzer");
        props.putIfAbsent(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.putIfAbsent(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
        props.putIfAbsent(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.putIfAbsent(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.putIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }

    public static void analyzeTraffic(final StreamsBuilder builder){
        final KStream<String, String> apacheLogs = builder.stream(INPUT_TOPIC);
        // Analyze traffic : date, time and url
        // Input example :
        // {"schema":{"type":"string","optional":false},"payload":"::1 - - [07/Jan/2024:16:10:53 +0100] \"GET /beam/index.html HTTP/1.1\" 200 3239 \"http://localhost/beam/contact.html\" \"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36\""}

        // Output example :
        // "07/Jan/2024:16:10:53 +0100,GET /beam/index.html HTTP/1.1"

        final KStream<String, String> trafficAnalysis = apacheLogs
        .flatMapValues(value -> {
            String[] parts = value.split(" ");
            String ip = parts[0];
            String date = parts[3].substring(1);
            String time = parts[4];
            // Remove time last char
            time = time.substring(0, time.length() - 1);
            String url = parts[5] + " " + parts[6] + " " + parts[7];
            // Replace double quotes
            url = url.replace("\"", "");
            
            value = ip + "," + date + "," + time + "," + url;
            System.out.println(value);
            return Arrays.asList(value);
        });

        trafficAnalysis.to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.String()));
    }
        
        

    public static void main(final String[] args) throws IOException {
        final Properties props = getStreamsConfig(args);

        final StreamsBuilder builder = new StreamsBuilder();
        analyzeTraffic(builder);
        final KafkaStreams streams = new KafkaStreams(builder.build(), props);
        final CountDownLatch latch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread("apache-log-analyzer-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (final Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}

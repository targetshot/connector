package app.targetshot.streams;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.TopicNameExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.regex.Pattern;

/**
 * Kafka Streams application that consumes all Vereins-Topics matching
 * &lt;Vereinsnummer&gt;.SMDB.(Schuetze|Treffer|Scheiben|Serien) and forwards
 * them to unified Confluent Cloud topics (e.g. ts.sds-test.schuetze).
 */
public final class TopicRouterApp {

    private static final Logger LOGGER = LoggerFactory.getLogger(TopicRouterApp.class);

    private static final Map<String, String> TARGET_SUFFIX = Map.of(
        ".SMDB.Schuetze", ".schuetze",
        ".SMDB.Treffer", ".treffer",
        ".SMDB.Scheiben", ".scheiben",
        ".SMDB.Serien", ".serien"
    );

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private TopicRouterApp() {
        // no-op
    }

    public static void main(String[] args) {
        final Config config = Config.fromEnv();
        final Properties props = buildProperties(config);
        final Topology topology = buildTopology(config);

        LOGGER.info("Starting TargetShot Streams with config: {}", config);
        final KafkaStreams streams = new KafkaStreams(topology, props);
        final CountDownLatch latch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOGGER.info("Shutdown signal received. Closing Kafka Streams.");
            streams.close(Duration.ofSeconds(10));
            latch.countDown();
        }));

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            LOGGER.error("Kafka Streams terminated with unrecoverable error", e);
            System.exit(1);
        }
        LOGGER.info("Kafka Streams stopped.");
    }

    private static Properties buildProperties(Config config) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, config.applicationId());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers());
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.AT_LEAST_ONCE);
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, Integer.toString(config.replicationFactor()));
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, Long.toString(config.commitIntervalMs()));
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, Integer.toString(config.numStreamThreads()));
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, config.cacheBytesBuffering());
        props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
            org.apache.kafka.streams.errors.LogAndContinueExceptionHandler.class);
        props.put(StreamsConfig.DEFAULT_PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG,
            org.apache.kafka.streams.errors.LogAndContinueExceptionHandler.class);
        props.put(StreamsConfig.RETRY_BACKOFF_MS_CONFIG, config.retryBackoffMs());
        props.put(StreamsConfig.RETRIES_CONFIG, config.producerRetries());
        props.put("auto.offset.reset", config.autoOffsetReset());
        return props;
    }

    private static Topology buildTopology(Config config) {
        StreamsBuilder builder = new StreamsBuilder();
        Pattern pattern = config.topicPattern();

        KStream<String, String> source = builder.stream(pattern, Consumed.with(Serdes.String(), Serdes.String()));

        TopicNameExtractor<String, String> extractor = (key, value, recordContext) -> {
            String sourceTopic = recordContext.topic();
            if (sourceTopic == null) {
                LOGGER.warn("Skipping record without topic (key={})", key);
                return null;
            }
            String normalized = normalizeTopic(sourceTopic, config.targetPrefix());
            if (normalized == null) {
                LOGGER.debug("Skipping unmatched topic {} (key={})", sourceTopic, key);
            }
            return normalized;
        };

        source
            .filter((key, value) -> value != null)
            .mapValues(TopicRouterApp::ensureJsonString)
            .to(extractor, Produced.with(Serdes.String(), Serdes.String()));

        return builder.build();
    }

    private static String ensureJsonString(String value) {
        if (value == null || value.isBlank()) {
            return value;
        }
        try {
            JsonNode node = OBJECT_MAPPER.readTree(value);
            return OBJECT_MAPPER.writeValueAsString(node);
        } catch (Exception ex) {
            // Value is not JSON or already a serialized string; return as-is
            return value;
        }
    }

    private static String normalizeTopic(String sourceTopic, String targetPrefix) {
        for (Map.Entry<String, String> entry : TARGET_SUFFIX.entrySet()) {
            if (sourceTopic.endsWith(entry.getKey())) {
                return targetPrefix + entry.getValue();
            }
        }
        return null;
    }

    private static final class Config {
        private static final String DEFAULT_PATTERN = "^[0-9]+\\.SMDB\\.(Schuetze|Treffer|Scheiben|Serien)$";

        private final String bootstrapServers;
        private final String applicationId;
        private final String targetPrefix;
        private final Pattern topicPattern;
        private final int replicationFactor;
        private final int numStreamThreads;
        private final long commitIntervalMs;
        private final String cacheBytesBuffering;
        private final String autoOffsetReset;
        private final int producerRetries;
        private final String retryBackoffMs;

        private Config(
            String bootstrapServers,
            String applicationId,
            String targetPrefix,
            Pattern topicPattern,
            int replicationFactor,
            int numStreamThreads,
            long commitIntervalMs,
            String cacheBytesBuffering,
            String autoOffsetReset,
            int producerRetries,
            String retryBackoffMs
        ) {
            this.bootstrapServers = bootstrapServers;
            this.applicationId = applicationId;
            this.targetPrefix = targetPrefix;
            this.topicPattern = topicPattern;
            this.replicationFactor = replicationFactor;
            this.numStreamThreads = numStreamThreads;
            this.commitIntervalMs = commitIntervalMs;
            this.cacheBytesBuffering = cacheBytesBuffering;
            this.autoOffsetReset = autoOffsetReset;
            this.producerRetries = producerRetries;
            this.retryBackoffMs = retryBackoffMs;
        }

        static Config fromEnv() {
            Map<String, String> env = System.getenv();

            String bootstrapServers = env.getOrDefault("STREAMS_BOOTSTRAP_SERVERS", "redpanda:9092");
            String applicationId = env.getOrDefault("STREAMS_APPLICATION_ID", "ts-streams-transform");
            String targetPrefix = env.getOrDefault("STREAMS_TARGET_PREFIX", "ts.sds-test").trim();
            if (targetPrefix.isEmpty()) {
                targetPrefix = "ts.sds-test";
            }
            String patternRaw = env.getOrDefault("STREAMS_INPUT_PATTERN", DEFAULT_PATTERN);
            Pattern compiledPattern = Pattern.compile(patternRaw);

            int replicationFactor = parsePositiveInt(env.get("STREAMS_REPLICATION_FACTOR"), 1);
            int numStreamThreads = parsePositiveInt(env.get("STREAMS_NUM_THREADS"), 1);
            long commitIntervalMs = parseLong(env.get("STREAMS_COMMIT_INTERVAL_MS"), 5000L);
            String cacheBytes = env.getOrDefault("STREAMS_CACHE_BYTES_BUFFERING", "10485760");
            String autoReset = env.getOrDefault("STREAMS_AUTO_OFFSET_RESET", "earliest").toLowerCase(Locale.ROOT);
            int producerRetries = parsePositiveInt(env.get("STREAMS_PRODUCER_RETRIES"), 10);
            String retryBackoffMs = env.getOrDefault("STREAMS_RETRY_BACKOFF_MS", "100");

            return new Config(
                bootstrapServers,
                applicationId,
                targetPrefix,
                compiledPattern,
                replicationFactor,
                numStreamThreads,
                commitIntervalMs,
                cacheBytes,
                autoReset,
                producerRetries,
                retryBackoffMs
            );
        }

        String bootstrapServers() {
            return bootstrapServers;
        }

        String applicationId() {
            return applicationId;
        }

        String targetPrefix() {
            return targetPrefix;
        }

        Pattern topicPattern() {
            return topicPattern;
        }

        int replicationFactor() {
            return replicationFactor;
        }

        int numStreamThreads() {
            return numStreamThreads;
        }

        long commitIntervalMs() {
            return commitIntervalMs;
        }

        String cacheBytesBuffering() {
            return cacheBytesBuffering;
        }

        String autoOffsetReset() {
            return autoOffsetReset;
        }

        int producerRetries() {
            return producerRetries;
        }

        String retryBackoffMs() {
            return retryBackoffMs;
        }

        private static int parsePositiveInt(String value, int defaultValue) {
            if (value == null || value.isBlank()) {
                return defaultValue;
            }
            try {
                int parsed = Integer.parseInt(value);
                return parsed > 0 ? parsed : defaultValue;
            } catch (NumberFormatException ex) {
                return defaultValue;
            }
        }

        private static long parseLong(String value, long defaultValue) {
            if (value == null || value.isBlank()) {
                return defaultValue;
            }
            try {
                return Long.parseLong(value);
            } catch (NumberFormatException ex) {
                return defaultValue;
            }
        }

        @Override
        public String toString() {
            return "Config{" +
                "bootstrapServers='" + bootstrapServers + '\'' +
                ", applicationId='" + applicationId + '\'' +
                ", targetPrefix='" + targetPrefix + '\'' +
                ", topicPattern=" + topicPattern.pattern() +
                ", replicationFactor=" + replicationFactor +
                ", numStreamThreads=" + numStreamThreads +
                ", commitIntervalMs=" + commitIntervalMs +
                ", cacheBytesBuffering='" + cacheBytesBuffering + '\'' +
                ", autoOffsetReset='" + autoOffsetReset + '\'' +
                ", producerRetries=" + producerRetries +
                ", retryBackoffMs='" + retryBackoffMs + '\'' +
                '}';
        }
    }
}

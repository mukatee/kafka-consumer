package osmo.monitoring.kafka.influx;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import osmo.monitoring.kafka.cassanda.CassandaAvroConsumer;
import osmo.monitoring.kafka.influx.avro.InFluxAvroConsumer;
import osmo.monitoring.kafka.influx.avro.SchemaRepository;
import osmo.monitoring.kafka.influx.json.InFluxJSONConsumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import osmo.monitoring.kafka.influx.telegraf.InFluxTelegrafConsumer;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @author Teemu Kanstren.
 */
public class Main {
  private final ConsumerConnector consumer;
  private ExecutorService executor;
  public static final String CONFIG_FILE = "kafka-importer.properties";
  private static final Logger log = LogManager.getLogger();

  public Main() {
    consumer = kafka.consumer.Consumer.createJavaConsumerConnector(createConsumerConfig());
  }

  public void shutdown() {
    if (consumer != null) consumer.shutdown();
    if (executor != null) executor.shutdown();
    try {
      if (!executor.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
        log.error("Timed out waiting for consumer threads to shut down, exiting uncleanly");
      }
    } catch (InterruptedException e) {
      log.error("Interrupted during shutdown, exiting uncleanly");
    }
  }

  public void run() {
    Map<String, Integer> topicCountMap = new HashMap<>();
    topicCountMap.put(Config.kafkaAvroTopic, Config.threads);
    topicCountMap.put(Config.kafkaJsonTopic, Config.threads);
    topicCountMap.put(Config.kafkaTelegrafTopic, Config.threads);
    Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
    //note: here we create number of threads that is the number per stream times the number of streams
    //this is required as each stream read blocks the thread it runs on..
    int nThreads = Config.threads * 3;
    executor = Executors.newFixedThreadPool(nThreads);
    log.info("Created executors");

    boolean influx = Config.consumerType.equals("influx");

    List<KafkaStream<byte[], byte[]>> avroStreams = consumerMap.get(Config.kafkaAvroTopic);
    SchemaRepository repo = new SchemaRepository();
    //now create objects to consume the messages
    for (final KafkaStream stream : avroStreams) {
      if (influx) executor.submit(new InFluxAvroConsumer(repo, stream));
      else executor.submit(new CassandaAvroConsumer(repo, stream));
      log.info("submitted avro tasks");
    }

    List<KafkaStream<byte[], byte[]>> jsonStreams = consumerMap.get(Config.kafkaJsonTopic);
    //now create objects to consume the messages
    for (final KafkaStream stream : jsonStreams) {
      if (influx) {
        executor.submit(new InFluxJSONConsumer(stream));
        log.info("submitted json task");
      } else {
        log.info("no json consumer for cassandra");
      }
    }

    List<KafkaStream<byte[], byte[]>> teleStreams = consumerMap.get(Config.kafkaTelegrafTopic);
    //now create objects to consume the messages
    for (final KafkaStream stream : teleStreams) {
      if (influx) {
        executor.submit(new InFluxTelegrafConsumer(stream));
        log.info("submitted telegraf task");
      } else {
        log.info("no telegraf consumer for cassandra");
      }
    }
  }


  private static ConsumerConfig createConsumerConfig() {
    Properties props = new Properties();
    props.put("zookeeper.connect", Config.zooUrl);
    props.put("group.id", Config.kafkaGroup);
    props.put("zookeeper.session.timeout.ms", "1000");
    props.put("zookeeper.sync.time.ms", "200");
    props.put("auto.commit.interval.ms", "1000");

    return new ConsumerConfig(props);
  }

  public static void init() throws Exception {
    File configFile = new File(CONFIG_FILE);
    if (!configFile.exists())
      throw new FileNotFoundException("Could not load configuration file " + CONFIG_FILE + " from current directory.");
    Properties props = new Properties();
    props.load(new FileInputStream(configFile));
    Config.kafkaAvroTopic = props.getProperty(Config.KEY_KAFKA_AVRO_TOPIC);
    Config.kafkaJsonTopic = props.getProperty(Config.KEY_KAFKA_JSON_TOPIC);
    Config.kafkaTelegrafTopic = props.getProperty(Config.KEY_KAFKA_TELEGRAF_TOPIC);
    Config.zooUrl = props.getProperty(Config.KEY_ZOOKEEPER_URL);
    Config.kafkaGroup = props.getProperty(Config.KEY_KAFKA_GROUP);
    Config.kafkaCluster = props.getProperty(Config.KEY_KAFKA_CLUSTER);
    Config.influxDbName = props.getProperty(Config.KEY_INFLUX_DB_NAME);
    Config.influxDbUrl = props.getProperty(Config.KEY_INFLUX_DB_URL);
    Config.influxUser = props.getProperty(Config.KEY_INFLUX_USERNAME);
    Config.influxPass = props.getProperty(Config.KEY_INFLUX_PASSWORD);
    Config.cassandraUrl = props.getProperty(Config.KEY_CASSANDRA_URL);
    Config.cassandraKeySpace = props.getProperty(Config.KEY_CASSANDRA_KEYSPACE);
    Config.cassandraReplicationFactor = props.getProperty(Config.KEY_CASSANDRA_REPLICATION_FACTOR);
    Config.consumerType = props.getProperty(Config.KEY_CONSUMER_TYPE);
    String threadsStr = props.getProperty(Config.KEY_THREAD_COUNT);
    if (threadsStr != null) Config.threads = Integer.parseInt(threadsStr);

    checkConfig();
  }

  private static void checkConfig() {
    String errors = "";
    if (Config.kafkaAvroTopic == null)
      errors += "Missing property '" + Config.KEY_KAFKA_AVRO_TOPIC + "' in configuration file " + CONFIG_FILE + "\n";
    if (Config.kafkaJsonTopic == null)
      errors += "Missing property '" + Config.KEY_KAFKA_JSON_TOPIC + "' in configuration file " + CONFIG_FILE + "\n";
    if (Config.kafkaTelegrafTopic == null)
      errors += "Missing property '" + Config.KEY_KAFKA_TELEGRAF_TOPIC + "' in configuration file " + CONFIG_FILE + "\n";
    if (Config.zooUrl == null)
      errors += "Missing property '" + Config.KEY_ZOOKEEPER_URL + "' in configuration file " + CONFIG_FILE + "\n";
    if (Config.kafkaGroup == null)
      errors += "Missing property '" + Config.KEY_KAFKA_GROUP + "' in configuration file " + CONFIG_FILE + "\n";
    if (Config.kafkaCluster == null)
      errors += "Missing property '" + Config.KEY_KAFKA_CLUSTER + "' in configuration file " + CONFIG_FILE + "\n";
    if (Config.influxDbName == null)
      errors += "Missing property '" + Config.KEY_INFLUX_DB_NAME + "' in configuration file " + CONFIG_FILE + "\n";
    if (Config.influxDbUrl == null)
      errors += "Missing property '" + Config.KEY_INFLUX_DB_URL + "' in configuration file " + CONFIG_FILE + "\n";
    if (Config.influxUser == null)
      errors += "Missing property '" + Config.KEY_INFLUX_USERNAME + "' in configuration file " + CONFIG_FILE + "\n";
    if (Config.influxPass == null)
      errors += "Missing property '" + Config.KEY_INFLUX_PASSWORD + "' in configuration file " + CONFIG_FILE + "\n";
    if (Config.threads == null)
      errors += "Missing property '" + Config.KEY_THREAD_COUNT + "' in configuration file " + CONFIG_FILE + "\n";
    if (Config.cassandraUrl == null)
      errors += "Missing property '" + Config.KEY_CASSANDRA_URL + "' in configuration file " + CONFIG_FILE + "\n";
    if (Config.cassandraKeySpace == null)
      errors += "Missing property '" + Config.KEY_CASSANDRA_KEYSPACE + "' in configuration file " + CONFIG_FILE + "\n";
    if (Config.consumerType == null)
      errors += "Missing property '" + Config.KEY_CONSUMER_TYPE + "' in configuration file " + CONFIG_FILE + "\n";
    else if (!Config.consumerType.equals("influx") && !Config.consumerType.equals("cassandra")) {
      errors += "Invalid value for property '" + Config.KEY_CONSUMER_TYPE + "' in configuration file " + CONFIG_FILE + ".\n";
      errors += "Allowed values are 'influx' and 'cassandra'. Was "+Config.consumerType+".\n";
    }
    if (Config.cassandraReplicationFactor == null)
      errors += "Missing property '" + Config.KEY_CASSANDRA_REPLICATION_FACTOR + "' in configuration file " + CONFIG_FILE + "\n";
    if (errors.length() > 0) throw new RuntimeException(errors);
  }

  public static void main(String[] args) throws Exception {
    init();
    log.info("Starting up consumer:" + Config.asString());
    Main main = new Main();
    main.run();
  }
}
package net.kanstren.kafka.influx;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import net.kanstren.kafka.influx.avro.InFluxAvroConsumer;
import net.kanstren.kafka.influx.avro.SchemaRepository;
import net.kanstren.kafka.influx.telegraf.InFluxTelegrafConsumer;
import net.kanstren.kafka.cassanda.CassandaAvroConsumer;
import net.kanstren.kafka.influx.json.InFluxJSONConsumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
 * Main access point to start up.
 *
 * @author Teemu Kanstren.
 */
public class Main {
  /** Kafka connector. */
  private final ConsumerConnector consumer;
  /** Thread pool. */
  private ExecutorService executor;
  /** Main configuration file. */
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

  /**
   * Practically the starting point.
   */
  public void run() {
    //create a map of what topics we want to get from Kafka and how many parallel streams we want to process them in
    Map<String, Integer> topicCountMap = new HashMap<>();
    int streams = 0;
    for (String avroTopic : Config.kafkaAvroTopics.keySet()) {
      topicCountMap.put(avroTopic, Config.threads);
      streams += Config.threads;
    }
    for (String avroTopic : Config.kafkaJsonTopics.keySet()) {
      topicCountMap.put(avroTopic, Config.threads);
      streams += Config.threads;
    }
    for (String avroTopic : Config.kafkaTelegrafTopics.keySet()) {
      topicCountMap.put(avroTopic, Config.threads);
      streams += Config.threads;
    }
    Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
    //note: here we create number of threads that is equal to the number of all the streams
    //this is required as each stream read blocks the thread it runs on..
    executor = Executors.newFixedThreadPool(streams);
    log.info("Created executors");

    //TODO: use a Const class
    boolean influx = Config.consumerType.equals("influx");

    SchemaRepository repo = new SchemaRepository();
    //attach database consumers for each Kafka Avro stream
    for (String avroTopic : Config.kafkaAvroTopics.keySet()) {
      List<KafkaStream<byte[], byte[]>> avroStreams = consumerMap.get(avroTopic);
      for (final KafkaStream stream : avroStreams) {
        if (influx) {
          String db = Config.kafkaAvroTopics.get(avroTopic);
          executor.submit(new InFluxAvroConsumer(repo, stream, db));
        }
        else executor.submit(new CassandaAvroConsumer(repo, stream));
        log.info("submitted avro tasks for "+avroTopic);
      }
    }

    //attach database consumers for each Kafka JSON stream
    for (String jsonTopic : Config.kafkaJsonTopics.keySet()) {
      List<KafkaStream<byte[], byte[]>> jsonStreams = consumerMap.get(jsonTopic);
      for (final KafkaStream stream : jsonStreams) {
        if (influx) {
          String db = Config.kafkaJsonTopics.get(jsonTopic);
          executor.submit(new InFluxJSONConsumer(stream, db));
          log.info("submitted json task for "+jsonTopic);
        } else {
          log.info("no json consumer for cassandra");
        }
      }
    }

    //attach database consumers for each Kafka Telegraf stream
    for (String teleTopic : Config.kafkaTelegrafTopics.keySet()) {
      List<KafkaStream<byte[], byte[]>> teleStreams = consumerMap.get(teleTopic);
      for (final KafkaStream stream : teleStreams) {
        if (influx) {
          String db = Config.kafkaTelegrafTopics.get(teleTopic);
          executor.submit(new InFluxTelegrafConsumer(stream, db));
          log.info("submitted telegraf task for "+teleTopic);
        } else {
          log.info("no telegraf consumer for cassandra");
        }
      }
    }
  }

  /**
   * Creates Kafka consumer configuration.
   * TODO: move these to the common configuration file.
   *
   * @return The consumer configuration for Kafka.
   */
  private static ConsumerConfig createConsumerConfig() {
    Properties props = new Properties();
    props.put("zookeeper.connect", Config.zooUrl);
    props.put("group.id", Config.kafkaGroup);
    props.put("zookeeper.session.timeout.ms", "1000");
    props.put("zookeeper.sync.time.ms", "200");
    props.put("auto.commit.interval.ms", "1000");

    return new ConsumerConfig(props);
  }

  /**
   * Read and set configuration options from the configuration file.
   *
   * @throws Exception If there is an error with reading the configuration or if the configuration is invalid.
   */
  public static void init() throws Exception {
    File configFile = new File(CONFIG_FILE);
    if (!configFile.exists())
      throw new FileNotFoundException("Could not load configuration file " + CONFIG_FILE + " from current directory.");
    Properties props = new Properties();
    props.load(new FileInputStream(configFile));
    init(props);
  }

  /**
   * Sets the configuration from the given properties. Separate to allow for unit tests.
   *
   * @param props The properties to set configuration from.
   * @throws Exception If there is an invalid configuration.
   */
  public static void init(Properties props) throws Exception {
    Config.dbPostFix = props.getProperty(Config.KEY_DB_POSTFIX);
    String errors = readKafkaTopics(props, Config.KEY_KAFKA_AVRO_TOPIC, Config.kafkaAvroTopics);
    errors += readKafkaTopics(props, Config.KEY_KAFKA_JSON_TOPIC, Config.kafkaJsonTopics);
    errors += readKafkaTopics(props, Config.KEY_KAFKA_TELEGRAF_TOPIC, Config.kafkaTelegrafTopics);
    Config.zooUrl = props.getProperty(Config.KEY_ZOOKEEPER_URL);
    Config.kafkaGroup = props.getProperty(Config.KEY_KAFKA_GROUP);
    Config.kafkaCluster = props.getProperty(Config.KEY_KAFKA_CLUSTER);
    Config.influxDbUrl = props.getProperty(Config.KEY_INFLUX_DB_URL);
    Config.influxUser = props.getProperty(Config.KEY_INFLUX_USERNAME);
    Config.influxPass = props.getProperty(Config.KEY_INFLUX_PASSWORD);
    Config.cassandraUrl = props.getProperty(Config.KEY_CASSANDRA_URL);
    Config.cassandraKeySpace = props.getProperty(Config.KEY_CASSANDRA_KEYSPACE);
    Config.cassandraReplicationFactor = props.getProperty(Config.KEY_CASSANDRA_REPLICATION_FACTOR);
    Config.consumerType = props.getProperty(Config.KEY_CONSUMER_TYPE);
    String threadsStr = props.getProperty(Config.KEY_THREAD_COUNT);
    if (threadsStr != null) Config.threads = Integer.parseInt(threadsStr);

    checkConfig(errors);
  }

  /**
   * Reads the Kafka topic names and matching InFluxDB database names from the configuration file.
   * These are expected to appear in order of name+index in the file.
   * Topic names are mapped to database names by expecting the database name to match topic name with a '_db' postfix.
   *
   * @param props Properties to set the topic names from.
   * @param topicBaseKey The base key for topic name (differs for avro/json/telegraf/...)
   * @param topicMap This is where the values read are stored.
   * @return Any errors in parsing. Empty string if no errors.
   */
  public static String readKafkaTopics(Properties props, String topicBaseKey, Map<String, String> topicMap) {
    String errors = "";
    int index = 1;
    String topicKey = topicBaseKey + index;
    String topic = props.getProperty(topicKey);
    String dbKey = topicKey + Config.dbPostFix;
    String db = props.getProperty(dbKey);
    while (topic != null) {
      if (db == null) {
        errors += "Missing database property for topic:"+topicKey+". Needs database name in '"+dbKey+"'.\n";
      }
      topicMap.put(topic, db);
      index++;
      topicKey = topicBaseKey + index;
      dbKey = topicKey+Config.dbPostFix;
      topic = props.getProperty(topicKey);
      db = props.getProperty(dbKey);
    }
    return errors;
  }

  /**
   * Check that all required configuration options are defined.
   * In case of any errors, an exception is thrown.
   *
   * @param errors Any previously observed errors to accumulate on top of.
   */
  private static void checkConfig(String errors) {
    if (Config.dbPostFix == null) {
      //this overwrites all previos errors if db prefix is not set. this is due to missing prefix causing issues with database name parsing.
      //TODO: remove this property and assume it to always be '_db'
      errors = "Missing property '" + Config.KEY_DB_POSTFIX + "' in configuration file " + CONFIG_FILE + ".\n";
    } else {
      if (Config.kafkaAvroTopics.size() == 0)
        errors += "Missing property '" + Config.KEY_KAFKA_AVRO_TOPIC + "' in configuration file " + CONFIG_FILE + ". Need to define at least one.\n";
      if (Config.kafkaJsonTopics.size() == 0)
        errors += "Missing property '" + Config.KEY_KAFKA_JSON_TOPIC + "' in configuration file " + CONFIG_FILE + ". Need to define at least one.\n";
      if (Config.kafkaTelegrafTopics.size() == 0)
        errors += "Missing property '" + Config.KEY_KAFKA_TELEGRAF_TOPIC + "' in configuration file " + CONFIG_FILE + ". Need to define at least one.\n";
    }
    if (Config.zooUrl == null)
      errors += "Missing property '" + Config.KEY_ZOOKEEPER_URL + "' in configuration file " + CONFIG_FILE + "\n";
    if (Config.kafkaGroup == null)
      errors += "Missing property '" + Config.KEY_KAFKA_GROUP + "' in configuration file " + CONFIG_FILE + "\n";
    if (Config.kafkaCluster == null)
      errors += "Missing property '" + Config.KEY_KAFKA_CLUSTER + "' in configuration file " + CONFIG_FILE + "\n";
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
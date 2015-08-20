package osmo.monitoring.kafka.influx;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import osmo.monitoring.kafka.influx.avro.InFluxAvroConsumer;
import osmo.monitoring.kafka.influx.avro.SchemaRepository;
import osmo.monitoring.kafka.influx.json.InFluxJSONConsumer;
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
 * @author Teemu Kanstren.
 */
public class Main {
  private final ConsumerConnector consumer;
  private ExecutorService executor;
  public static final String CONFIG_FILE = "kafka-importer.properties";
  private static final Logger log = LogManager.getLogger();
  private static boolean avro = true;

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
    topicCountMap.put(Config.kafkaTopic, Config.threads);
    Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
    List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(Config.kafkaTopic);

    // now launch all the threads
    executor = Executors.newFixedThreadPool(Config.threads);

    log.info("Created executors");

    SchemaRepository repo = new SchemaRepository();
    // now create an object to consume the messages
    for (final KafkaStream stream : streams) {
      if (avro) executor.submit(new InFluxAvroConsumer(repo, stream));
      else executor.submit(new InFluxJSONConsumer(stream));
//      executor.submit(new ConsoleConsumer(stream, clusterName));
      log.info("submitted task");
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

  private static void init() throws Exception {
    File configFile = new File(CONFIG_FILE);
    if (!configFile.exists())
      throw new FileNotFoundException("Could not load configuration file " + CONFIG_FILE + " from current directory.");
    Properties props = new Properties();
    props.load(new FileInputStream(configFile));
    Config.kafkaTopic = props.getProperty(Config.KEY_KAFKA_TOPIC);
    Config.zooUrl = props.getProperty(Config.KEY_ZOOKEEPER_URL);
    Config.kafkaGroup = props.getProperty(Config.KEY_KAFKA_GROUP);
    Config.kafkaCluster = props.getProperty(Config.KEY_KAFKA_CLUSTER);
    Config.influxDbName = props.getProperty(Config.KEY_INFLUX_DB_NAME);
    Config.influxDbUrl = props.getProperty(Config.KEY_INFLUX_DB_URL);
    Config.influxUser = props.getProperty(Config.KEY_INFLUX_USERNAME);
    Config.influxPass = props.getProperty(Config.KEY_INFLUX_PASSWORD);
    String threadsStr = props.getProperty(Config.KEY_THREAD_COUNT);
    if (threadsStr != null) Config.threads = Integer.parseInt(threadsStr);

    String serializer = props.getProperty("serializer");
    if (serializer == null) throw new RuntimeException("Missing configuration 'serializer' in configuration file "+CONFIG_FILE+". Should be 'avro' or 'json'.");
    serializer = serializer.toLowerCase();
    if (serializer.equals("json")) avro=false;
    else if (serializer.equals("avro")) avro = true;
    else throw new RuntimeException("Invalid configuration value for 'serializer' in configuration file "+CONFIG_FILE+" ("+serializer+"). Should be 'avro' or 'json'.");

    checkConfig();
  }

  private static void checkConfig() {
    String errors = "";
    if (Config.kafkaTopic == null)
      errors += "Missing property '" + Config.KEY_KAFKA_TOPIC + "' in configuration file " + CONFIG_FILE + "\n";
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
    if (errors.length() > 0) throw new RuntimeException(errors);
  }

  public static void main(String[] args) throws Exception {
    init();
    log.info("Starting up consumer:" + Config.asString());
    Main main = new Main();
    main.run();
  }
}
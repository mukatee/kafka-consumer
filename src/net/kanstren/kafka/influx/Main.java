package net.kanstren.kafka.influx;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

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

  public Main() {
    consumer = kafka.consumer.Consumer.createJavaConsumerConnector(createConsumerConfig());
  }

  public void shutdown() {
    if (consumer != null) consumer.shutdown();
    if (executor != null) executor.shutdown();
    try {
      if (!executor.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
        System.out.println("Timed out waiting for consumer threads to shut down, exiting uncleanly");
      }
    } catch (InterruptedException e) {
      System.out.println("Interrupted during shutdown, exiting uncleanly");
    }
  }

  public void run() {
    Map<String, Integer> topicCountMap = new HashMap<>();
    topicCountMap.put(Config.kafkaTopic, Config.threads);
    Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
    List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(Config.kafkaTopic);

    // now launch all the threads
    executor = Executors.newFixedThreadPool(Config.threads);

    System.out.println("Created executors");

    // now create an object to consume the messages
    for (final KafkaStream stream : streams) {
      executor.submit(new InFluxConsumer(stream));
//      executor.submit(new ConsoleConsumer(stream, clusterName));
      System.out.println("submitted task");
    }
  }

  private static ConsumerConfig createConsumerConfig() {
    Properties props = new Properties();
    props.put("zookeeper.connect", Config.zooUrl);
    props.put("group.id", Config.zooGroup);
    props.put("zookeeper.session.timeout.ms", "1000");
    props.put("zookeeper.sync.time.ms", "200");
    props.put("auto.commit.interval.ms", "1000");

    return new ConsumerConfig(props);
  }

  private static void init() throws Exception {
    File configFile = new File(CONFIG_FILE);
    if (!configFile.exists()) throw new FileNotFoundException("Could not load configuration file "+CONFIG_FILE+" from current directory.");
    Properties props = new Properties();
    props.load(new FileInputStream(configFile));
    Config.kafkaTopic = props.getProperty(Config.KEY_KAFKA_TOPIC);
    Config.zooUrl = props.getProperty(Config.KEY_ZOOKEEPER_URL);
    Config.zooGroup = props.getProperty(Config.KEY_ZOOKEEPER_GROUP);
    Config.zooCluster = props.getProperty(Config.KEY_ZOOKEEPER_CLUSTER);
    Config.influxDbName = props.getProperty(Config.KEY_INFLUX_DB_NAME);
    Config.influxDbUrl = props.getProperty(Config.KEY_INFLUX_DB_URL);
    Config.influxUser = props.getProperty(Config.KEY_INFLUX_USERNAME);
    Config.influxPass = props.getProperty(Config.KEY_INFLUX_PASSWORD);
    String threadsStr = props.getProperty(Config.KEY_THREAD_COUNT);
    if (threadsStr != null) Config.threads = Integer.parseInt(threadsStr);
    checkConfig();
  }

  private static void checkConfig() {
    String errors = "";
    if (Config.kafkaTopic == null) errors += "Missing property '"+Config.KEY_KAFKA_TOPIC+"' in configuration file "+CONFIG_FILE+"\n";
    if (Config.zooUrl == null) errors += "Missing property '"+Config.KEY_ZOOKEEPER_URL+"' in configuration file "+CONFIG_FILE+"\n";
    if (Config.zooGroup == null) errors += "Missing property '"+Config.KEY_ZOOKEEPER_GROUP+"' in configuration file "+CONFIG_FILE+"\n";
    if (Config.zooCluster == null) errors += "Missing property '"+Config.KEY_ZOOKEEPER_CLUSTER+"' in configuration file "+CONFIG_FILE+"\n";
    if (Config.influxDbName == null) errors += "Missing property '"+Config.KEY_INFLUX_DB_NAME+"' in configuration file "+CONFIG_FILE+"\n";
    if (Config.influxDbUrl == null) errors += "Missing property '"+Config.KEY_INFLUX_DB_URL+"' in configuration file "+CONFIG_FILE+"\n";
    if (Config.influxUser == null) errors += "Missing property '"+Config.KEY_INFLUX_USERNAME+"' in configuration file "+CONFIG_FILE+"\n";
    if (Config.influxPass == null) errors += "Missing property '"+Config.KEY_INFLUX_PASSWORD+"' in configuration file "+CONFIG_FILE+"\n";
    if (Config.threads == null) errors += "Missing property '"+Config.KEY_THREAD_COUNT+"' in configuration file "+CONFIG_FILE+"\n";
    if (errors.length() > 0 ) throw new RuntimeException(errors);
  }

  public static void main(String[] args) throws Exception {
    init();
    System.out.println("Starting up consumer:" + Config.asString());
    Main main = new Main();
    main.run();
  }
}
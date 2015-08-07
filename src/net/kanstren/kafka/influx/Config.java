package net.kanstren.kafka.influx;

/**
 * Defines used configurations options, as well as configuration file keys.
 *
 * @author Teemu Kanstren.
 */
public class Config {
  /** For defining the Kafka topic to listen to. */
  public static final String KEY_KAFKA_TOPIC = "kafka_topic";
  /** For defining the URL where the Kafka Zookeeper is running. */
  public static final String KEY_ZOOKEEPER_URL = "zookeeper_url";
  /** For defining the Kafka group name. */
  public static final String KEY_KAFKA_GROUP = "kafka_group";
  /** For defining Kafka cluster name. */
  public static final String KEY_KAFKA_CLUSTER = "kafka_cluster";
  /** For defining the number of parallel threads to use in consumer. */
  public static final String KEY_THREAD_COUNT = "thread_count";
  /** For defining the Influx DB name to connect to. */
  public static final String KEY_INFLUX_DB_NAME = "influx_db_name";
  /** For defining the Influx DB URL to connect to. */
  public static final String KEY_INFLUX_DB_URL = "influx_db_url";
  /** For defining the user name to connect to the Influx DB server. */
  public static final String KEY_INFLUX_USERNAME = "influx_username";
  /** For defining the password to connect to the InFlux DB server. */
  public static final String KEY_INFLUX_PASSWORD = "influx_password";

  /** URL for the Kafka Zookeeper */
  public static String zooUrl = null;
  /** Kafka group name to use. */
  public static String kafkaGroup = null;
  /** Kafka cluster name to use. */
  public static String kafkaCluster = null;
  /** The Kafka topic where we receive messages. */
  public static String kafkaTopic = null;
  /** Number of parallel threads to use for processing messages. */
  public static Integer threads = null;
  /** Name of thet Influx DB where to store the measurements. */
  public static String influxDbName = null;
  /** URL for the InfluxDB server. */
  public static String influxDbUrl = null;
  /** User name for the Influx DB. */
  public static String influxUser = null;
  /** Password for the Influx DB. */
  public static String influxPass = null;

  public static String asString() {
    return "Config{" +
            "zooUrl='" + zooUrl + '\'' +
            ", kafkaGroup='" + kafkaGroup + '\'' +
            ", kafkaCluster='" + kafkaCluster + '\'' +
            ", kafkaTopic='" + kafkaTopic + '\'' +
            ", threads=" + threads +
            ", influxDbName='" + influxDbName + '\'' +
            ", influxDbUrl='" + influxDbUrl + '\'' +
            ", influxUser='" + influxUser + '\'' +
            ", influxPass='" + influxPass + '\'' +
            '}';
  }
}

package net.kanstren.kafka.influx;

import java.util.HashMap;
import java.util.Map;

/**
 * Defines used configurations options, as well as configuration file keys.
 *
 * @author Teemu Kanstren.
 */
public class Config {
  /** For defining the Kafka topics to listen to for avro messages. */
  public static final String KEY_KAFKA_AVRO_TOPIC = "kafka_avro_topic";
  /** For defining the Influx database names for the Kafka topics. */
  public static final String KEY_DB_POSTFIX = "db_postfix";
  /** For defining the Kafka topic to listen to for json messages. */
  public static final String KEY_KAFKA_JSON_TOPIC = "kafka_json_topic";
  /** For defining the Kafka topic to listen to for telegraf messages. */
  public static final String KEY_KAFKA_TELEGRAF_TOPIC = "kafka_telegraf_topic";
  /** For defining the URL where the Kafka Zookeeper is running. */
  public static final String KEY_ZOOKEEPER_URL = "zookeeper_url";
  /** For defining the Kafka group name. */
  public static final String KEY_KAFKA_GROUP = "kafka_group";
  /** For defining Kafka cluster name. */
  public static final String KEY_KAFKA_CLUSTER = "kafka_cluster";
  /** For defining the number of parallel threads to use in consumer. */
  public static final String KEY_THREAD_COUNT = "thread_count";
  /** For defining the Influx DB URL to connect to. */
  public static final String KEY_INFLUX_DB_URL = "influx_db_url";
  /** For defining the user name to connect to the Influx DB server. */
  public static final String KEY_INFLUX_USERNAME = "influx_username";
  /** For defining the password to connect to the InFlux DB server. */
  public static final String KEY_INFLUX_PASSWORD = "influx_password";
  /** Address for Cassandra server. */
  public static final String KEY_CASSANDRA_URL = "cassandra_url";
  /** Name of Cassandra keyspace under which to store measurements. */
  public static final String KEY_CASSANDRA_KEYSPACE = "cassandra_keyspace";
  /** Cassandra keyspace replication factor. */
  public static final String KEY_CASSANDRA_REPLICATION_FACTOR = "cassandra_replication_factor";
  /** Type of consumer. Currently "influx" or "cassandra". */
  public static final String KEY_CONSUMER_TYPE = "consumer_type";

  /** URL for the Kafka Zookeeper */
  public static String zooUrl = null;
  /** Kafka group name to use. */
  public static String kafkaGroup = null;
  /** Kafka cluster name to use. */
  public static String kafkaCluster = null;
  /** The Kafka topics where we receive avro messages, and their associated database names.
   * Key = topic name, Value = database name. */
  public static Map<String, String> kafkaAvroTopics = new HashMap<>();
  /** The Kafka topics where we receive json messages, and their associated database names.
   * Key = topic name, Value = database name. */
  public static Map<String, String> kafkaJsonTopics = new HashMap<>();
  /** The Kafka topics where we receive telegraf messages, and their associated database names.
   * Key = topic name, Value = database name. */
  public static Map<String, String> kafkaTelegrafTopics = new HashMap<>();
  /** Number of parallel threads to use for processing messages. */
  public static Integer threads = null;
  /** URL for the InfluxDB server. */
  public static String influxDbUrl = null;
  /** User name for the Influx DB. */
  public static String influxUser = null;
  /** Password for the Influx DB. */
  public static String influxPass = null;
  /** Postfix value to use for database key property names. */
  public static String dbPostFix = null;
  /** URL for Cassandra. */
  public static String cassandraUrl = null;
  /** Cassandra keyspace name. Like a database name. */
  public static String cassandraKeySpace = null;
  /** Cassandra configuration options. */
  public static String cassandraReplicationFactor = null;
  /** Are we expecting to dump data into InfluxDB or Cassandra? */
  public static String consumerType = null;

  public static String asString() {
    return "Config{" +
            "zooUrl='" + zooUrl + '\'' +
            ", kafkaGroup='" + kafkaGroup + '\'' +
            ", kafkaCluster='" + kafkaCluster + '\'' +
            ", kafkaAvroTopics='" + kafkaAvroTopics + '\'' +
            ", kafkaJsonTopics='" + kafkaJsonTopics + '\'' +
            ", kafkaTelegrafTopics='" + kafkaTelegrafTopics + '\'' +
            ", threads=" + threads +
            ", influxDbUrl='" + influxDbUrl + '\'' +
            ", influxUser='" + influxUser + '\'' +
            ", influxPass='" + influxPass + '\'' +
            ", cassandraUrl='" + cassandraUrl + '\'' +
            ", cassandraKeySpace='" + cassandraKeySpace + '\'' +
            ", cassandraReplicationFactor='" + cassandraReplicationFactor + '\'' +
            ", consumerType='" + consumerType + '\'' +
            '}';
  }
}

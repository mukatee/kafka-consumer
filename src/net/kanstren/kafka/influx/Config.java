package net.kanstren.kafka.influx;

/**
 * @author Teemu Kanstren.
 */
public class Config {
  public static final String KEY_KAFKA_TOPIC = "kafka_topic";
  public static final String KEY_ZOOKEEPER_URL = "zookeeper_url";
  public static final String KEY_ZOOKEEPER_GROUP = "zookeeper_group";
  public static final String KEY_ZOOKEEPER_CLUSTER = "zookeeper_cluster";
  public static final String KEY_THREAD_COUNT = "thread_count";
  public static final String KEY_INFLUX_DB_NAME = "influx_db_name";
  public static final String KEY_INFLUX_DB_URL = "influx_db_url";
  public static final String KEY_INFLUX_USERNAME = "influx_username";
  public static final String KEY_INFLUX_PASSWORD = "influx_password";

  public static String zooUrl = null;
  public static String zooGroup = null;
  public static String zooCluster = null;
  public static String kafkaTopic = null;
  public static Integer threads = null;
  public static String influxDbName = null;
  public static String influxDbUrl = null;
  public static String influxUser = null;
  public static String influxPass = null;

  public static String asString() {
    return "Config{" +
            "zooUrl='" + zooUrl + '\'' +
            ", zooGroup='" + zooGroup + '\'' +
            ", zooCluster='" + zooCluster + '\'' +
            ", kafkaTopic='" + kafkaTopic + '\'' +
            ", threads=" + threads +
            ", influxDbName='" + influxDbName + '\'' +
            ", influxDbUrl='" + influxDbUrl + '\'' +
            ", influxUser='" + influxUser + '\'' +
            ", influxPass='" + influxPass + '\'' +
            '}';
  }
}

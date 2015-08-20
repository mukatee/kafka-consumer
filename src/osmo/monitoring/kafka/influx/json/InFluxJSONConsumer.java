package osmo.monitoring.kafka.influx.json;

import com.eclipsesource.json.JsonObject;
import com.eclipsesource.json.JsonValue;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import osmo.monitoring.kafka.influx.Config;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import osmo.monitoring.kafka.influx.Config;

import java.util.concurrent.TimeUnit;

/**
 * Kafka consumer listening to measurement data.
 * Reads the Kafka topic stream and stores received data to the configured InfluxDB instance.
 * <p>
 * Expected data format is messages with a JSON data structure.
 * This should have a header and a body section.
 * The header has the generic elements, the body measurement specifics.
 * <p>
 * Example message content:
 * <p>
 * {
 * "header": {
 * "db": "session1",
 * "type": "free ram",
 * "tom": "router",
 * "time": 2
 * },
 * "body": {
 * "target": "127.0.0.1:55",
 * "oid": "1.1.1.1.1",
 * "value": 11
 * }
 * }
 * <p>
 * Header Fields are (all required):
 * -"db": The name of the InfluxDB database where this measurement should be stored. To support e.g. multiple tests run in parallel stored in different DB instances.
 * -"type": The type of the measurement. Used in InfluxDB to set the measurement data type stored. Spaces convered to underscores, so "free ram" type becomes "free_ram" in the database.
 * -"tom": Short for "Target of Measurement". Intended to identify what is being measured.
 * -"time": Measurement time as Epoch milliseconds.
 * <p>
 * The "db" is used to selected the DB, the "type" to define measurement type, "tom" as a measurement tag, and time as the measurement time value.
 * <p>
 * Body fields (one of "value" or "str_value" is required):
 * -"value": If this exists, it is stored as the measurement value in InfluxDB.
 * -Any others: Stored as tags for the measurement.
 * <p>
 * TODO: check if parallel streams read the same data or not.
 *
 * @author Teemu Kanstren.
 */
public class InFluxJSONConsumer implements Runnable {
  /** The Kafka measurement data stream. */
  private final KafkaStream stream;
  /** Identifier for the thread this consumer is running on. */
  private final int id;
  /** The Influx DB driver instance. */
  private final InfluxDB db;
  /** To create unique thread id values. */
  private static int nextId = 1;
  private static final Logger log = LogManager.getLogger();

  public InFluxJSONConsumer(KafkaStream stream) {
    this.stream = stream;
    this.id = nextId++;
    db = InfluxDBFactory.connect(Config.influxDbUrl, Config.influxUser, Config.influxPass);
    db.createDatabase(Config.influxDbName);
  }

  @Override
  public void run() {
    try {
      runrun();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * To avoid too many try-catches this is separate..
   */
  public void runrun() {
    System.out.println("Waiting to consume data");
    ConsumerIterator<byte[], byte[]> it = stream.iterator();
    while (it.hasNext()) {
      String msg = new String(it.next().message());
      System.out.println("Thread " + id + ":: " + msg);
      process(msg);
    }
    System.out.println("Shutting down consumer Thread: " + id);
  }

  public void process(String msg) {
    if (!msg.startsWith("{")) {
      System.out.println("Skipping non JSON input");
      return;
    }
    JsonObject json = JsonObject.readFrom(msg);
    JsonObject header = json.get("header").asObject();
    long time = header.get("time").asLong();
    header.remove("time");
    String type = header.get("type").asString();
    header.remove("type");
    type = type.replace(' ', '_');
    String dbName = header.get("db").asString();
    header.remove("db");

    JsonObject body = json.get("body").asObject();

    BatchPoints batchPoints = BatchPoints
            .database(Config.influxDbName)
            .tag("async", "true")
            .retentionPolicy("default")
            .consistency(InfluxDB.ConsistencyLevel.ALL)
            .build();

    if (type.equals("multipoint")) {
      multiPoint(batchPoints, time, header, body);
    } else {
      singlePoint(batchPoints, type, time, header, body);
    }

    db.write(batchPoints);
  }

  private void multiPoint(BatchPoints batch, long time, JsonObject header, JsonObject body) {
    for (JsonObject.Member member : body) {
      String name = member.getName();
      Point.Builder builder = Point.measurement(name);
      builder.time(time, TimeUnit.MILLISECONDS);
      setTags(header, builder);
      setField("value", builder, member.getValue());
      Point point = builder.build();
      batch.point(point);
    }
  }

  private void singlePoint(BatchPoints batch, String type, long time, JsonObject header, JsonObject body) {
    Point.Builder builder = Point.measurement(type);
    builder.time(time, TimeUnit.MILLISECONDS);
    setTags(header, builder);
    createFields(body, builder);
    Point point = builder.build();
    System.out.println("writing point:" + point);
    batch.point(point);
  }

  private void setTags(JsonObject header, Point.Builder builder) {
    for (JsonObject.Member member : header) {
      String value = member.getValue().asString();
//      value = value.replace(' ', '_');
      builder.tag(member.getName(), value);
    }
  }

  private void createFields(JsonObject body, Point.Builder builder) {
    for (JsonObject.Member member : body) {
      setField(member.getName(), builder, member.getValue());
    }
  }

  private void setField(String name, Point.Builder builder, JsonValue jsonValue) {
    try {
      long value = jsonValue.asLong();
      builder.field(name, value);
      return;
    } catch (Exception e) {
    }
    try {
      double value = jsonValue.asDouble();
      builder.field(name, value);
      return;
    } catch (Exception e) {
    }
    try {
      boolean value = jsonValue.asBoolean();
      builder.field(name, value);
      return;
    } catch (Exception e) {
    }
    String value = jsonValue.asString();
    /** TODO: fix space escaping. */
    value = value.replace(' ', '_');
    builder.field(name, value);
  }
}

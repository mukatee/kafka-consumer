package net.kanstren.kafka.influx.telegraf;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import net.kanstren.kafka.influx.InFlux;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.influxdb.InfluxDB;
import org.influxdb.dto.Point;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Kafka consumer listening to measurement data in the format of the Telegraf tool (InfluxDB line protocol).
 * Reads the Kafka topic stream and stores received data to the configured InfluxDB instance.
 * For protocol spec see https://influxdb.com/docs/v0.9/write_protocols/line.html.
 *
 * @author Teemu Kanstren.
 */
public class InFluxTelegrafConsumer implements Runnable {
  private static final Logger log = LogManager.getLogger();
  /** The Kafka measurement data stream. */
  private final KafkaStream stream;
  /** Identifier for the thread this consumer is running on. */
  private final int id;
  /** Influx database name. */
  private final String dbName;
  /** The Influx DB driver instance. */
  private final InfluxDB db;
  /** To create unique thread id values. */
  private static int nextId = 1;
  /** For tracking the number of processed measurements. */
  private int count = 0;
  /** If a measurement value is in this set, it is considered a boolean measurement with value "true". From InfluxDB lineprotocol spec. */
  private List<String> trueSet = Arrays.asList("T", "t", "true", "TRUE");

  /** A testing constructor */
  public InFluxTelegrafConsumer() {
    id = 1;
    db = null;
    dbName = null;
    stream = null;
  }

  public InFluxTelegrafConsumer(KafkaStream stream, String dbName) {
    this.stream = stream;
    this.id = nextId++;
    db = InFlux.influxFor(dbName);
    this.dbName = dbName;
  }

  @Override
  public void run() {
    try {
      runrun();
    } catch (Exception e) {
      log.error("Telegraf consumer crash", e);
    }
  }

  /**
   * To avoid too many try-catches this is separate..
   */
  public void runrun() throws IOException {
    log.info("Waiting to consume data");
    ConsumerIterator<byte[], byte[]> it = stream.iterator();
    while (it.hasNext()) {
      byte[] msg = it.next().message();
//      log.trace("Thread " + id + ":: " + Arrays.toString(msg));
      process(new String(msg, "UTF8"));
    }
    log.info("Shutting down Telegraf consumer Thread: " + id);
  }

  /**
   * Parses a single line received in the InfluxDB line protocol format as delivered by Telegraf.
   *
   * Example line: net_drop_out,best=teemu,tom=teemun\ kone,interface=en0 value=0i 1434055562000000000
   *
   * The start of the line before the first comma is the measurement name (net_drop_out).
   *
   * The following part of the line until the space (' ') character is the set of metadata tags.
   * If one of these tags contains a space it is escaped as "\ ".
   * In the above example line the following tags are present
   * "best"="teemu",
   * "tom"="teemun kone"
   * "interface"="en0"
   *
   * The following parts are set of data fields and values for the measurement.
   * Here this is a single field named "value" with the value of "0i".
   * This is a numerical value of integer format identified by starting number and trailing "i".
   * For more details on data types see the {@link #setValue} method.
   *
   * The last part of the line (after last space character) is the timestamp of the measurement (milliseconds).
   *
   * @param line The data line to parse.
   * @return A parsed set of objects for the given measurement line.
   */
  public TelegrafMeasure parse(String line) {
    //first we replace all whitespace in measurements/tags with "_". That is "\ " becomes "_"
    line = line.replaceAll("\\\\ ", "_");
//    System.out.println("line:"+line);
    //splitting with whitespace we get measurement and tags at index 0, fields in index 1 and timestamp in index 2
    String[] split = line.split(" ");
    long time = Long.parseLong(split[2]);
    String keyValue = split[1];
    String[] kvSplit = keyValue.split("=");
    //we assume telegraf always sends just one field value in one line
    String fieldName = kvSplit[0];
    String fieldValue = kvSplit[1];
    String measureAndTags = split[0];
    Map<String, String> tags = new HashMap<>();
    String measure = parseMeasureAndTags(measureAndTags, tags);
    String tom = tags.get("tom");
    if (tom == null) {
      throw new IllegalArgumentException("Target of measurement ('host' tag) required: missing for:"+line);
    }
    return new TelegrafMeasure(measure, time, fieldName, fieldValue, tags);
  }

  /**
   * Parse the actual measurement value(s) as well as tags from the InfluxDB lineprotocol.
   *
   * @param measureAndTags The line to parse.
   * @param tags This is where parsed tags are stored. Key = tag name, value = tag value.
   * @return The measurement name.
   */
  public String parseMeasureAndTags(String measureAndTags, Map<String, String> tags) {
    String[] split = measureAndTags.split(",");
    for (int i = 1 ; i < split.length ; i++) {
      String tagKeyValue = split[i];
      String[] tagTeam = tagKeyValue.split("=");
      tags.put(tagTeam[0], tagTeam[1]);
    }
    tags.put("tom", tags.remove("host"));
    return split[0];
  }

  /**
   * Procses a Telegraf message received over Kafka.
   *
   * @param msg The msg to process.
   */
  public void process(String msg) {
    TelegrafMeasure measure = parse(msg);
    Point.Builder builder = Point.measurement(measure.name);
    for (Map.Entry<String, String> entry : measure.tags.entrySet()) {
      builder.tag(entry.getKey(), entry.getValue());
    }
    setValue(builder, measure);
    Point point = builder.build();
//    log.trace("Writing to InFlux:"+point);
    db.write(dbName, "default", point);
    count++;
    if (count % 100 == 0) System.out.print(count + ",");
    if (count % 1000 == 0) System.out.println();
  }

  /**
   * Set value with correct type for InfluxDB based on given Telegraf measure.
   *
   * @param builder The InfluxDB interface.
   * @param measure The measure to store.
   */
  private void setValue(Point.Builder builder, TelegrafMeasure measure) {
    String value = measure.fieldValue;
    String name = measure.fieldName;
    if (value.startsWith("\"")) {
      //its a string
      builder.field(name, value);
      return;
    }
    if (value.endsWith("i")) {
      //its an integer
      String substring = value.substring(0, value.length() - 1);
      builder.field(name, Long.parseLong(substring));
      return;
    }
    if (value.charAt(0) >= 48 && value.charAt(0) <= 57) {
      //it starts with a number (ascii codes for 0-9), but did not end with 'i', so it is a float
      builder.field(name, Float.parseFloat(value.substring(0, value.length())));
      return;
    }
    //its a boolean..
    boolean b = trueSet.contains(value);
    builder.field(name, b);
  }

  /**
   * A class encapsulating measurement data provided by Telegraf.
   * Assumes that Telegraf provides all measures with a single field.
   */
  public static final class TelegrafMeasure {
    /** Name of the measurement, e.g. CPU load. */
    public final String name;
    /** Measurement time. When was this recorded? */
    public final long time;
    /** Field name for the measurement data. */
    public final String fieldName;
    /** Value of the field. */
    public final String fieldValue;
    /** Metadata tags for the measurements. Key = tag name, value = tag value. E.g., "tom"="host1". */
    public final Map<String, String> tags;

    public TelegrafMeasure(String name, long time, String fieldName, String fieldValue, Map<String, String> tags) {
      this.name = name;
      this.time = time;
      this.fieldName = fieldName;
      this.fieldValue = fieldValue;
      this.tags = tags;
    }
  }
}

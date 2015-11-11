package osmo.monitoring.kafka.influx.avro;

import com.eclipsesource.json.JsonObject;
import com.eclipsesource.json.JsonValue;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import org.apache.avro.Schema;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import osmo.monitoring.kafka.influx.Config;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import osmo.monitoring.kafka.influx.Config;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Kafka consumer listening to measurement data.
 * Reads the Kafka topic stream and stores received data to the configured InfluxDB instance.
 *
 * @author Teemu Kanstren.
 */
public class InFluxAvroConsumer implements Runnable {
  private static final Logger log = LogManager.getLogger();
  /** The Kafka measurement data stream. */
  private final KafkaStream stream;
  /** Identifier for the thread this consumer is running on. */
  private final int id;
  /** The Influx DB driver instance. */
  private final InfluxDB db;
  /** To create unique thread id values. */
  private static int nextId = 1;
  private final SchemaRepository repo;
  private int count = 0;

  public InFluxAvroConsumer(SchemaRepository repo, KafkaStream stream) {
    this.repo = repo;
    this.stream = stream;
    this.id = nextId++;
    db = InfluxDBFactory.connect(Config.influxDbUrl, Config.influxUser, Config.influxPass);
    db.enableBatch(2000, 1, TimeUnit.SECONDS);
//    db.setLogLevel(InfluxDB.LogLevel.HEADERS);
    db.createDatabase(Config.influxDbName);
  }

  @Override
  public void run() {
    try {
      runrun();
    } catch (Exception e) {
      log.error("Avro consumer crash", e);
    }
  }

  /**
   * To avoid too many try-catches this is separate..
   */
  public void runrun() {
    log.info("Waiting to consume data");
    ConsumerIterator<byte[], byte[]> it = stream.iterator();
    while (it.hasNext()) {
      byte[] msg = it.next().message();
      if (msg.length < 2) {
        log.info("ignoring short msg, assuming topic polling");
        continue;
      }
//      log.trace("Thread " + id + ":: " + Arrays.toString(msg));
      process(msg);
    }
    log.info("Shutting down consumer Thread: " + id);
  }

  public void process(byte[] msg) {
    byte schemaId = msg[0];
    Schema schema = repo.schemaFor(schemaId);
    GenericDatumReader<GenericRecord> reader = repo.readerFor(schemaId);
    //TODO: test for handling of invalid id values (not in repo)
    Schema.Field headerField = schema.getField("header");
    Schema headerSchema = headerField.schema();
    List<Schema.Field> headerFields = headerSchema.getFields();
    Schema.Field bodyField = schema.getField("body");
    Schema bodySchema = bodyField.schema();
    List<Schema.Field> bodyFields = bodySchema.getFields();

    Decoder d = DecoderFactory.get().binaryDecoder(msg, 1, msg.length - 1, null);
    try {
      GenericRecord record = reader.read(null, d);
      GenericRecord header = (GenericRecord)record.get("header");
      GenericRecord body = (GenericRecord)record.get("body");

      multiPoint(header, body, headerFields, bodyFields);
//      log.trace("Stored msg:"+record);
    } catch (Exception e) {
      log.error("Error while processing received Kafka msg. Skipping this msg:"+ Arrays.toString(msg), e);
    }
  }

  private void multiPoint(GenericRecord header, GenericRecord body, List<Schema.Field> headerFields, List<Schema.Field> bodyFields) {
    String type = header.get("type").toString();
    type = type.replace(' ', '_');
    boolean multipoint = type.startsWith("_");
    if (multipoint) type = type.substring(1);
    long time = (Long) header.get("time");
    Map<String, String> tags = new HashMap<>();
    for (Schema.Field field : headerFields) {
      String tagName = field.name();
      Object value = header.get(tagName);
      if (tagName.equals("type") || tagName.equals("time")) continue;
      if (value == null) continue;
      tags.put(tagName, value.toString());
    }
    if (multipoint) {
      int counter = getObjectCounter(body);
      //TODO: check if multipoint is really necessary or if we can graph the data in other ways from influx + grafana (sparse data graphing)
      for (Schema.Field field : bodyFields) {
        String fieldName = field.name();
//        if (fieldName.equals("object_counter")) continue;
        Object value = body.get(fieldName);
        if (value == null) continue;
        Point.Builder builder = Point.measurement(type+"_"+fieldName);
        builder.time(time, TimeUnit.MILLISECONDS);
//      builder.field("value", value);
        for (Map.Entry<String, String> entry : tags.entrySet()) {
          builder.tag(entry.getKey(), entry.getValue());
        }
        setValue(builder, field.schema(), value);
        if (counter > 0) {
          builder.tag("object_id", ""+counter);
        }
        Point point = builder.build();
        log.trace("Writing to InFlux:"+point);
        db.write(Config.influxDbName, "default", point);
        count++;
      }
    } else {
      for (Schema.Field field : bodyFields) {
        String fieldName = field.name();
        Object value = body.get(fieldName);
        if (value == null) continue;
        Point.Builder builder = null;
        try {
          builder = Point.measurement(type);
        } catch (Throwable e) {
          e.printStackTrace();
        }
        builder.time(time, TimeUnit.MILLISECONDS);
        for (Map.Entry<String, String> entry : tags.entrySet()) {
          builder.tag(entry.getKey(), entry.getValue());
        }
        setValue(builder, field.schema(), value);
        Point point = builder.build();
//        log.trace("Writing to InFlux:"+point);
        db.write(Config.influxDbName, "default", point);
        count++;
      }
    }
    if (count % 100 == 0) System.out.print("AVRO:"+count+",");
    if (count % 1000 == 0) System.out.println();
  }

  private int getObjectCounter(GenericRecord body) {
    Object value = body.get("object_counter");
    if (value == null) {
      return -1;
    }
    return (int)body.get("object_counter");
  }

  private void setValue(Point.Builder builder, Schema schema, Object value) {
    switch (schema.getType()) {
      case DOUBLE:
        if (value instanceof String) {
          log.warn("Expected double for "+schema.getName()+", got string:"+value+". Ignoring value.");
          break;
        }
        builder.field("value", value);
        break;
      case FLOAT:
        if (value instanceof String) {
          log.warn("Expected float for "+schema.getName()+", got string:"+value+". Ignoring value.");
          break;
        }
        builder.field("value", value);
        break;
      case INT:
        if (value instanceof String) {
          log.warn("Expected integer for "+schema.getName()+", got string:"+value+". Ignoring value.");
          break;
        }
        builder.field("value", value);
        break;
      case LONG:
        if (value instanceof String) {
          log.warn("Expected long for "+schema.getName()+", got string:"+value+". Ignoring value.");
          break;
        }
        builder.field("value", value);
        break;
      case STRING:
        builder.field("value", value.toString());
        break;
      case UNION:
        List<Schema> types = schema.getTypes();
        for (Schema option : types) {
          Schema.Type type = option.getType();
          if (type.equals(Schema.Type.NULL)) continue;
          setValue(builder, option, value);
        }
        break;
      default:
        throw new IllegalArgumentException("Unsupported field type:"+schema.getType());
    }
  }

}

package net.kanstren.kafka.influx.avro;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import org.apache.avro.Schema;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.influxdb.InfluxDB;
import org.influxdb.dto.Point;
import net.kanstren.kafka.influx.InFlux;

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
  /** Influx database name. */
  private final String dbName;
  /** The Influx DB driver instance. */
  private final InfluxDB db;
  /** To create unique thread id values. */
  private static int nextId = 1;
  /** Avro schema repository for decoding messages from Kafka. */
  private final SchemaRepository repo;
  /** For tracking the number of received messages. */
  private int count = 0;

  public InFluxAvroConsumer(SchemaRepository repo, KafkaStream stream, String dbName) {
    this.repo = repo;
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
      log.error("Avro consumer crash", e);
    }
  }

  /**
   * To avoid too many try-catches this is separate..
   */
  public void runrun() {
    log.info("Waiting to consume data");
    ConsumerIterator<byte[], byte[]> it = stream.iterator();
    //loop through all messages in the stream
    while (it.hasNext()) {
      byte[] msg = it.next().message();
      if (msg.length < 2) {
        //valid messages are longer than 2 bytes as the first one is schema id
        //once upon time some libraries (pypro) would start with a short message to try if the kafka topic was alive. this is what topic polling refers to.
        log.info("ignoring short msg, assuming topic polling");
        continue;
      }
//      log.trace("Thread " + id + ":: " + Arrays.toString(msg));
      process(msg);
    }
    log.info("Shutting down consumer Thread: " + id);
  }

  /**
   * Process messages from Kafka.
   *
   * @param msg The received message as a byte array.
   */
  public void process(byte[] msg) {
    //each message must start with one byte identifying the Avro schema do decode it
    byte schemaId = msg[0];
    //using the schema id we get the actual schema to decode with
    Schema schema = repo.schemaFor(schemaId);
    GenericDatumReader<GenericRecord> reader = repo.readerFor(schemaId);
    //TODO: test for handling of invalid id values (not in repo)
    //each message is assumed to have a header and body with specific fields
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

  /**
   * Stores the received measurement data in InfluxDB.
   * Some measures are split into several and stored separately. These are identified by the leading _ character in the measurement type.
   */
  private void multiPoint(GenericRecord header, GenericRecord body, List<Schema.Field> headerFields, List<Schema.Field> bodyFields) {
    String type = header.get("type").toString();
    //no whitespace in measurement type for InfluxDB
    type = type.replace(' ', '_');
    //leading _ identifies the data as something to split into separate measurements
    boolean multipoint = type.startsWith("_");
    if (multipoint) type = type.substring(1);
    long time = (Long) header.get("time");
    Map<String, String> tags = new HashMap<>();
    //everything in the header is considered as metadata and used as tags for InfluxDB
    for (Schema.Field field : headerFields) {
      String tagName = field.name();
      Object value = header.get(tagName);
      //we already processed type specifically and time so lets not tag those
      if (tagName.equals("type") || tagName.equals("time")) continue;
      if (value == null) continue;
      tags.put(tagName, value.toString());
    }
    if (multipoint) {
      //TODO: check why we need object_id (if we do...)
      int counter = getObjectCounter(body);
      //we can either store data in influx as a single measurement with several fields or several measurements with single field
      //some data is sparse, meaning different fields may be missing at different times. those we split to several depending on available fields.
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
        db.write(dbName, "default", point);
        count++;
      }
    } else {
      //here we handle single point cases, meaning we use a single measurement with as many fields as needed
      for (Schema.Field field : bodyFields) {
        String fieldName = field.name();
        Object value = body.get(fieldName);
        if (value == null) continue;
        Point.Builder builder = Point.measurement(type);
        builder.time(time, TimeUnit.MILLISECONDS);
        for (Map.Entry<String, String> entry : tags.entrySet()) {
          builder.tag(entry.getKey(), entry.getValue());
        }
        setValue(builder, field.schema(), value);
        Point point = builder.build();
//        log.trace("Writing to InFlux:"+point);
        db.write(dbName, "default", point);
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
        builder.field("value", value);
        break;
      case FLOAT:
        builder.field("value", value);
        break;
      case INT:
        builder.field("value", value);
        break;
      case LONG:
        builder.field("value", value);
        break;
      case STRING:
        builder.field("value", value.toString());
        break;
      case UNION:
        //optional avro elements are represented as unions of null and the actual type
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

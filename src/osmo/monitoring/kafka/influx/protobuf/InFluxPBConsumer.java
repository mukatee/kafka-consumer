package osmo.monitoring.kafka.influx.protobuf;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import osmo.monitoring.kafka.influx.Config;
import pypro.snmp.PyproSnmpFloat;
import pypro.snmp.PyproSnmpInt;
import pypro.snmp.PyproSnmpStr;

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
public class InFluxPBConsumer implements Runnable {
  private static final Logger log = LogManager.getLogger();
  /** The Kafka measurement data stream. */
  private final KafkaStream stream;
  /** Identifier for the thread this consumer is running on. */
  private final int id;
  /** The Influx DB driver instance. */
  private final InfluxDB db;
  /** To create unique thread id values. */
  private static int nextId = 1;

  public InFluxPBConsumer(KafkaStream stream) {
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
      log.trace("Thread " + id + ":: " + Arrays.toString(msg));
      process(msg);
    }
    log.info("Shutting down consumer Thread: " + id);
  }

  public void process(byte[] msg) {
    byte schemaId = msg[0];

    try {
      Object msgObj = null;
      ByteString bs = ByteString.copyFrom(msg, 1, msg.length - 1);
      BatchPoints batch = BatchPoints
              .database(Config.influxDbName)
              .tag("async", "true")
              .retentionPolicy("default")
              .consistency(InfluxDB.ConsistencyLevel.ALL)
              .build();
      Point point = null;
      switch (schemaId) {
        case 1:
          PyproSnmpInt.SNMPInteger snmpInt = PyproSnmpInt.SNMPInteger.parseFrom(bs);
          point = store(snmpInt);
          msgObj = snmpInt;
          break;
        case 2:
          PyproSnmpFloat.SNMPFloat snmpFloat = PyproSnmpFloat.SNMPFloat.parseFrom(bs);
          point = store(snmpFloat);
          msgObj = snmpFloat;
          break;
        case 3:
          PyproSnmpStr.SNMPString snmpString = PyproSnmpStr.SNMPString.parseFrom(bs);
          point = store(snmpString);
          msgObj = snmpString;
          break;
        default:
          throw new IllegalArgumentException("Unknown schema identifier:"+schemaId);
      }
      batch.point(point);

      db.write(batch);
      log.trace("Stored msg:"+msgObj);
    } catch (InvalidProtocolBufferException e) {
      log.error("Error while processing received Kafka msg ("+ Arrays.toString(msg)+"). Skipping this msg.", e);
    }
  }

  private Point store(PyproSnmpStr.SNMPString snmpString) {
    PyproSnmpStr.Header header = snmpString.getHeader();
    long time = header.getTime();
    String type = header.getType();
    type = type.replace(' ', '_');
    String address = header.getAddress();
    String oid = header.getOid();
    String tom = header.getTom();

    PyproSnmpStr.Body body = snmpString.getBody();
    String value = body.getValue();

    Point.Builder builder = Point.measurement(type);
    builder.time(time, TimeUnit.MILLISECONDS);
    builder.tag("tom", tom);
    builder.tag("oid", oid);
    builder.tag("address", address);
    builder.field("value", value);
    return builder.build();
  }

  private Point store(PyproSnmpFloat.SNMPFloat snmpFloat) {
    PyproSnmpFloat.Header header = snmpFloat.getHeader();
    long time = header.getTime();
    String type = header.getType();
    type = type.replace(' ', '_');
    String address = header.getAddress();
    String oid = header.getOid();
    String tom = header.getTom();

    PyproSnmpFloat.Body body = snmpFloat.getBody();
    double value = body.getValue();

    Point.Builder builder = Point.measurement(type);
    builder.time(time, TimeUnit.MILLISECONDS);
    builder.tag("tom", tom);
    builder.tag("oid", oid);
    builder.tag("address", address);
    builder.field("value", value);
    return builder.build();
  }

  private Point store(PyproSnmpInt.SNMPInteger snmpInt) {
    PyproSnmpInt.Header header = snmpInt.getHeader();
    long time = header.getTime();
    String type = header.getType();
    type = type.replace(' ', '_');
    String address = header.getAddress();
    String oid = header.getOid();
    String tom = header.getTom();

    PyproSnmpInt.Body body = snmpInt.getBody();
    long value = body.getValue();

    Point.Builder builder = Point.measurement(type);
    builder.time(time, TimeUnit.MILLISECONDS);
    builder.tag("tom", tom);
    builder.tag("oid", oid);
    builder.tag("address", address);
    builder.field("value", value);

    Point point = builder.build();
    return point;
  }
}

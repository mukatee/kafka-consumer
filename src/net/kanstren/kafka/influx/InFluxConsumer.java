package net.kanstren.kafka.influx;

import com.eclipsesource.json.JsonObject;
import com.eclipsesource.json.JsonValue;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.utils.Json;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;

import java.util.Date;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

/**
 * @author Teemu Kanstren.
 */
public class InFluxConsumer implements Runnable {
  private final KafkaStream stream;
  private final int id;
  private final InfluxDB db;
  private static int nextId = 1;

  public InFluxConsumer(KafkaStream stream) {
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

  public void runrun() {
    System.out.println("Waiting to consume data");
    ConsumerIterator<byte[], byte[]> it = stream.iterator();
    while (it.hasNext()) {
      String msg = new String(it.next().message());
      System.out.println("Thread " + id + ": " + msg);
      if (!msg.startsWith("{")) {
        System.out.println("Skipping non JSON input");
        continue;
      }
      JsonObject json = JsonObject.readFrom(msg);
      JsonObject header = json.get("header").asObject();
      long time = header.get("time").asLong();
      String type = header.get("type").asString();
      type = type.replace(' ', '_');
      JsonObject body = json.get("body").asObject();
      String dbName = header.get("db").asString();
      String tom = header.get("tom").asString();

      BatchPoints batchPoints = BatchPoints
              .database(Config.influxDbName)
//              .time(time, TimeUnit.MILLISECONDS)
              .tag("async", "true")
              .retentionPolicy("default")
              .consistency(InfluxDB.ConsistencyLevel.ALL)
              .build();

      Point.Builder builder = Point.measurement(type);
      builder.tag("tom", tom);
//      JsonValue value = body.get("value");
//      if (value == null) value = JsonValue.valueOf(-1);
      JsonValue jsonValue = body.get("value");
      if (jsonValue != null) {
        setValue(builder, jsonValue);
        body.remove("value");
      } else {
        //sometimes some measures are numeric other times not (e.g., errors from SNMP agent seem to come and go..)
        jsonValue = body.get("str_value");
        String value = jsonValue.asString();
        value = value.replace(' ', '_');
        builder.field("str_value", value);
        body.remove("str_value");
      }
      builder.time(time, TimeUnit.MILLISECONDS);
      for (JsonObject.Member member : body) {
        String value = member.getValue().asString();
        value = value.replace(' ', '_');
        builder.tag(member.getName(), value);
      }
      Point point = builder.build();
      System.out.println("writing point:"+point);
      batchPoints.point(point);
      batchPoints.point(point);
//      batchPoints.point(point2);
      db.write(batchPoints);
    }
    System.out.println("Shutting down consumer Thread: " + id);
  }

  private void setValue(Point.Builder builder, JsonValue jsonValue) {
    String name = "value";
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
    value = value.replace(' ', '_');
    builder.field(name, value);
  }
}

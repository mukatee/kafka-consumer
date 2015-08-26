package osmo.monitoring.kafka.influx;

import osmo.monitoring.kafka.influx.json.InFluxJSONConsumer;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import osmo.common.TestUtils;

import java.util.List;
import java.util.Map;

import static org.testng.Assert.assertEquals;

/**
 * @author Teemu Kanstren.
 */
public class InFluxJSONTests {
  private InFluxJSONConsumer influx = null;
  private InfluxDB db = null;

  @BeforeMethod
  public void setup() {
    Config.influxDbName = "unit_test_db";
    Config.influxUser = "my_test_user";
    Config.influxPass = "my_test_pw";
    Config.influxDbUrl = "http://192.168.2.153:8086";
    db = InfluxDBFactory.connect(Config.influxDbUrl, Config.influxUser, Config.influxPass);
    db.deleteDatabase(Config.influxDbName);
    influx = new InFluxJSONConsumer(null);
  }

  @AfterMethod
  public void cleanup() {

  }

  @Test
  public void errorMsg() {
    String msg = TestUtils.getResource(InFluxJSONTests.class, "test_msg_error.json");
    influx.process(msg);
    QueryResult result = db.query(new Query("select * from test_oid3", Config.influxDbName));
    checkGenerals(result, 1, "1.1.1.2.1", "127.0.0.1:155", "test target 3", "error", "test_oid3");
  }

  private void checkGenerals(QueryResult result, int count, String oid, String target, String tom, String type, String expectedName) {
    //https://github.com/influxdb/influxdb-java/blob/master/src/main/java/org/influxdb/dto/QueryResult.java
    List<QueryResult.Result> items = result.getResults();
    assertEquals(items.size(), 1, "Number of results");
    QueryResult.Result item1 = items.get(0);
    List<QueryResult.Series> series = item1.getSeries();
    assertEquals(series.size(), 1, "Number of series");
    QueryResult.Series series1 = series.get(0);
    Map<String, String> tags = series1.getTags();
    assertEquals(tags.size(), 4, "Number of tags created");
    assertEquals(tags.get("oid"), oid);
    assertEquals(tags.get("target"), target);
    assertEquals(tags.get("tom"), tom);
    //fourth tag is influxdb internal
    List<List<Object>> values = series1.getValues();
    assertEquals(values.size(), count, "Number of values");
    List<String> columns = series1.getColumns();
    assertEquals(columns.toString(), "[time, "+type+"]", "Columns created");
    String name = series1.getName();
    assertEquals(name, expectedName);
  }

  @Test
  public void floatMeasure() {
    String msg = TestUtils.getResource(InFluxJSONTests.class, "test_msg_cpu_float.json");
    influx.process(msg);
    QueryResult result = db.query(new Query("select * from cpu_load", Config.influxDbName));
    checkGenerals(result, 1, "1.1.1.2.1", "127.0.0.1:155", "server01", "value", "cpu_load");
  }

  @Test
  public void floatMeasures2() {
    String msg = TestUtils.getResource(InFluxJSONTests.class, "test_msg_cpu_float.json");
    influx.process(msg);
    msg = TestUtils.getResource(InFluxJSONTests.class, "test_msg_cpu_int.json");
    influx.process(msg);
    QueryResult result = db.query(new Query("select * from cpu_load", Config.influxDbName));
    checkGenerals(result, 2, "1.1.1.2.1", "127.0.0.1:155", "server01", "value", "cpu_load");
  }
}

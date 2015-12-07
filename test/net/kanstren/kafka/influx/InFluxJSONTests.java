package net.kanstren.kafka.influx;

import net.kanstren.kafka.TestData;
import net.kanstren.kafka.influx.avro.InFluxAvroConsumer;
import net.kanstren.kafka.influx.avro.SchemaRepository;
import net.kanstren.kafka.influx.json.InFluxJSONConsumer;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import osmo.common.TestUtils;

import java.util.List;
import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

/**
 * @author Teemu Kanstren.
 */
public class InFluxJSONTests {
  private InFluxJSONConsumer influx = null;
  private InfluxDB db = null;
  private String dbName = null;

  @BeforeClass
  public void fixtureSetup() throws Exception {
    Main.init();
    Config.influxUser = "my_test_user";
    Config.influxPass = "my_test_pw";
    Config.influxBatch = false;
  }

  @BeforeMethod
  public void setup() throws Exception {
    TestData.index++;
    dbName = "unit_test_db_"+TestData.index;
//    Config.influxDbUrl = "http://192.168.2.153:8086";
    db = InfluxDBFactory.connect(Config.influxDbUrl, Config.influxUser, Config.influxPass);
//    db.deleteDatabase(dbName);
    influx = new InFluxJSONConsumer(null, dbName);
  }

  @AfterMethod
  public void drop() {
    db.deleteDatabase(dbName);
  }

  @Test
  public void errorMsg() {
    String msg = TestUtils.getResource(InFluxJSONTests.class, "test_msg_error.json");
    influx.process(msg);
    QueryResult result = db.query(new Query("select * from test_oid3", dbName));
    assertMeasure(result, 0, 0, 1, 1, "1.1.1.2.1", "127.0.0.1:155", "test target 3", "error", "test_oid3", "value_could_not_be_read");
  }

  private void assertMeasure(QueryResult result, int serieIndex, int measureIndex, int valueCount, int serieCount, String oid, String address,
                             String tom, String type, String expectedName, Object expectedValue) {
    List<QueryResult.Result> items = result.getResults();
    assertEquals(items.size(), 1, "Number of results");
    QueryResult.Result item1 = items.get(0);
    List<QueryResult.Series> series = item1.getSeries();
    assertNotNull(series, "Series should not be null in "+item1);
    assertEquals(series.size(), serieCount, "Number of series");
    QueryResult.Series series1 = series.get(serieIndex);
    List<List<Object>> valuesList = series1.getValues();
    assertEquals(valuesList.size(), valueCount, "Number of values");
//    Map<String, String> tags = series1.getTags();
//    assertEquals(tags.size(), 4, "Number of tags created");
//    assertEquals(tags.get("oid"), oid);
//    assertEquals(tags.get("address"), address);
//    assertEquals(tags.get("tom"), tom);
    //fourth tag is influxdb internal
    List<String> columns = series1.getColumns();
    assertEquals(columns.toString(), "[time, "+type+", oid, target, tom]", "Columns created");
    List<Object> measures = valuesList.get(measureIndex);
    assertEquals(measures.size(), 5, "Number of measures"); //this is "time", "value", oid, address, tom
    Object value = measures.get(columns.indexOf(type));
    assertEquals(value, expectedValue, "Value for measure "+measureIndex+" in series "+serieIndex);
    assertEquals(measures.get(columns.indexOf("oid")), oid);
    assertEquals(measures.get(columns.indexOf("target")), address);
    assertEquals(measures.get(columns.indexOf("tom")), tom);
    String name = series1.getName();
    assertEquals(name, expectedName);
  }

  @Test
  public void floatMeasure() {
    String msg = TestUtils.getResource(InFluxJSONTests.class, "test_msg_cpu_float.json");
    influx.process(msg);
    QueryResult result = db.query(new Query("select * from cpu_load", dbName));
    assertMeasure(result, 0, 0, 1, 1, "1.1.1.2.1", "127.0.0.1:155", "server01", "data_value", "cpu_load", "2.0");
  }

  @Test
  public void floatMeasures2() {
    String msg = TestUtils.getResource(InFluxJSONTests.class, "test_msg_cpu_float.json");
    influx.process(msg);
    msg = TestUtils.getResource(InFluxJSONTests.class, "test_msg_cpu_int.json");
    influx.process(msg);
    QueryResult result = db.query(new Query("select * from cpu_load", dbName));
    assertMeasure(result, 0, 0, 2, 1, "1.1.1.2.1", "127.0.0.1:155", "server01", "data_value", "cpu_load", "2.0");
    assertMeasure(result, 0, 1, 2, 1, "1.1.1.2.1", "127.0.0.1:155", "server01", "data_value", "cpu_load", "3");
  }
}

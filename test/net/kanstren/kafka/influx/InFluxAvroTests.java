package net.kanstren.kafka.influx;

import net.kanstren.kafka.TestData;
import net.kanstren.kafka.influx.avro.InFluxAvroConsumer;
import net.kanstren.kafka.influx.avro.SchemaRepository;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import pypro.snmp.SNMPFloat;
import pypro.snmp.SNMPFloatBody;
import pypro.snmp.SNMPFloatHeader;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.util.List;
import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

/**
 * @author Teemu Kanstren.
 */
public class InFluxAvroTests {
  private InFluxAvroConsumer influx = null;
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
    influx = new InFluxAvroConsumer(new SchemaRepository(), null, dbName);
  }

  @AfterMethod
  public void drop() {
    db.deleteDatabase(dbName);
  }

  @Test
  public void floatMeasure() throws Exception {
    processFloat(1.1, "127.0.0.1", "email server", "1.1.1.2.1", 11111L, "cpu load");

    Thread.sleep(2000);
    QueryResult result = db.query(new Query("select * from cpu_load", dbName));
    assertMeasure(result, 0, 0, 1, 1, "1.1.1.2.1", "127.0.0.1", "email server", "value", "cpu_load", 1.1);
  }

  @Test
  public void floatMeasures2() throws Exception {
    processFloat(1.1, "127.0.0.1", "email server", "1.1.1.2.1", 11111L, "cpu load");
    processFloat(1.2, "127.0.0.2", "email server", "1.1.1.2.1", 11111L, "cpu load");

    QueryResult result = db.query(new Query("select * from cpu_load", dbName));
    assertMeasure(result, 0, 0, 2, 1, "1.1.1.2.1", "127.0.0.1", "email server", "value", "cpu_load", 1.1);
    assertMeasure(result, 0, 1, 2, 1, "1.1.1.2.1", "127.0.0.2", "email server", "value", "cpu_load", 1.2);
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
    assertEquals(columns.toString(), "[time, address, oid, tom, "+type+"]", "Columns created");
    List<Object> measures = valuesList.get(measureIndex);
    assertEquals(measures.size(), 5, "Number of measures"); //this is "time", "value", oid, address, tom
    Object value = measures.get(columns.indexOf(type));
    assertEquals(value, expectedValue, "Value for measure "+measureIndex+" in series "+serieIndex);
    assertEquals(measures.get(columns.indexOf("oid")), oid);
    assertEquals(measures.get(columns.indexOf("address")), address);
    assertEquals(measures.get(columns.indexOf("tom")), tom);
    String name = series1.getName();
    assertEquals(name, expectedName);
  }

  private SNMPFloat createFloatMsg(String addr, String tom, String oid, long time, String type) {
    SNMPFloat snmp = new SNMPFloat();
    SNMPFloatHeader header = new SNMPFloatHeader();
    header.setAddress(addr);
    header.setTom(tom);
    header.setOid(oid);
    header.setTime(time);
    header.setType(type);

    SNMPFloatBody body = new SNMPFloatBody();

    snmp.setHeader(header);
    snmp.setBody(body);
    return snmp;
  }

  private void processFloat(double value, String addr, String tom, String oid, long time, String type) throws Exception {
    SNMPFloat snmp = createFloatMsg(addr, tom, oid, time, type);
    SNMPFloatBody body = snmp.getBody();
    body.setValue(value);

    byte[] msg = transform(snmp, "pypro_snmp_float");
    influx.process(msg);
  }

  private byte[] transform(SNMPFloat snmp, String schemaName) throws Exception {
    Schema schema = new Schema.Parser().parse(new FileInputStream("schemas/"+schemaName+".avsc"));
    SpecificDatumWriter<SNMPFloat> writer = new SpecificDatumWriter<>(schema);
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    os.write(1);
    BinaryEncoder enc = EncoderFactory.get().binaryEncoder(os, null);
    writer.write(snmp, enc);
    enc.flush();
    byte[] data = os.toByteArray();
    return data;
  }
}

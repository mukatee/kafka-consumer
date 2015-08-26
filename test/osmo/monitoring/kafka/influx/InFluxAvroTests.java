package osmo.monitoring.kafka.influx;

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import osmo.common.TestUtils;
import osmo.monitoring.kafka.influx.avro.InFluxAvroConsumer;
import osmo.monitoring.kafka.influx.avro.SchemaRepository;
import osmo.monitoring.kafka.influx.json.InFluxJSONConsumer;
import pypro.snmp.SNMPBody;
import pypro.snmp.SNMPHeader;
import pypro.snmp.SNMPMeasure;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.testng.Assert.assertEquals;

/**
 * @author Teemu Kanstren.
 */
public class InFluxAvroTests {
  private InFluxAvroConsumer influx = null;
  private InfluxDB db = null;

  @BeforeMethod
  public void setup() {
    Config.influxDbName = "unit_test_db";
    Config.influxUser = "my_test_user";
    Config.influxPass = "my_test_pw";
    Config.influxDbUrl = "http://192.168.2.153:8086";
    db = InfluxDBFactory.connect(Config.influxDbUrl, Config.influxUser, Config.influxPass);
    db.deleteDatabase(Config.influxDbName);
    influx = new InFluxAvroConsumer(new SchemaRepository(), null);
  }

  @AfterMethod
  public void cleanup() {

  }

  @Test
  public void errorMsg() {
  }

  @Test
  public void floatMeasure() throws Exception {
    processFloat(1.1, "127.0.0.1", "email server", "1.1.1.2.1", 11111L, "cpu load");

    QueryResult result = db.query(new Query("select * from cpu_load", Config.influxDbName));
    assertMeasure(result, 0, 0, 1, 1, "1.1.1.2.1", "127.0.0.1", "email server", "value", "cpu_load", 1.1);
  }

  @Test
  public void floatMeasures2() throws Exception {
    processFloat(1.1, "127.0.0.1", "email server", "1.1.1.2.1", 11111L, "cpu load");
    processFloat(1.2, "127.0.0.2", "email server", "1.1.1.2.1", 11111L, "cpu load");

    QueryResult result = db.query(new Query("select * from cpu_load", Config.influxDbName));
    assertMeasure(result, 0, 0, 1, 2, "1.1.1.2.1", "127.0.0.1", "email server", "value", "cpu_load", 1.1);
    assertMeasure(result, 1, 0, 1, 2, "1.1.1.2.1", "127.0.0.2", "email server", "value", "cpu_load", 1.2);
  }

  private void assertMeasure(QueryResult result, int serieIndex, int measureIndex, int valueCount, int serieCount, String oid, String address,
                             String tom, String type, String expectedName, Object expectedValue) {
    List<QueryResult.Result> items = result.getResults();
    assertEquals(items.size(), 1, "Number of results");
    QueryResult.Result item1 = items.get(0);
    List<QueryResult.Series> series = item1.getSeries();
    assertEquals(series.size(), serieCount, "Number of series");
    QueryResult.Series series1 = series.get(serieIndex);
    List<List<Object>> values = series1.getValues();
    assertEquals(values.size(), valueCount, "Number of values");
    Map<String, String> tags = series1.getTags();
    assertEquals(tags.size(), 4, "Number of tags created");
    assertEquals(tags.get("oid"), oid);
    assertEquals(tags.get("address"), address);
    assertEquals(tags.get("tom"), tom);
    //fourth tag is influxdb internal
    List<String> columns = series1.getColumns();
    assertEquals(columns.toString(), "[time, "+type+"]", "Columns created");
    List<Object> measures = values.get(measureIndex);
    assertEquals(measures.size(), 2, "Number of measures"); //this is "time" and "value"
    Object value = measures.get(columns.indexOf(type));
    assertEquals(value, expectedValue, "Value for measure "+measureIndex+" in series "+serieIndex);
    String name = series1.getName();
    assertEquals(name, expectedName);
  }

  private SNMPMeasure createMsg(String addr, String tom, String oid, long time, String type) {
    SNMPMeasure snmp = new SNMPMeasure();
    SNMPHeader header = new SNMPHeader();
    header.setAddr(addr);
    header.setTom(tom);
    header.setOid(oid);
    header.setTime(time);
    header.setType(type);

    SNMPBody body = new SNMPBody();

    snmp.setHeader(header);
    snmp.setBody(body);
    return snmp;
  }

  private void processFloat(double value, String addr, String tom, String oid, long time, String type) throws Exception {
    SNMPMeasure snmp = createMsg(addr, tom, oid, time, type);
    SNMPBody body = snmp.getBody();
    body.setValueFloat(value);

    byte[] msg = transform(snmp);
    influx.process(msg);
  }

  private byte[] transform(SNMPMeasure snmp) throws Exception {
    Schema schema = new Schema.Parser().parse(new FileInputStream("schemas/pypro_snmp.avsc"));
    SpecificDatumWriter<SNMPMeasure> writer = new SpecificDatumWriter<>(schema);
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    os.write(1);
    BinaryEncoder enc = EncoderFactory.get().binaryEncoder(os, null);
    writer.write(snmp, enc);
    enc.flush();
    byte[] data = os.toByteArray();
    return data;
  }
}

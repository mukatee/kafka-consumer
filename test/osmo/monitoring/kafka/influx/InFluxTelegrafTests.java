package osmo.monitoring.kafka.influx;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import osmo.monitoring.kafka.influx.telegraf.InFluxTelegrafConsumer;

import static org.testng.Assert.assertEquals;


/**
 * @author Teemu Kanstren.
 */
public class InFluxTelegrafTests {
  private InFluxTelegrafConsumer consumer = null;

  @BeforeMethod
  public void setup() {
    consumer = new InFluxTelegrafConsumer();
  }

  @Test
  public void lineParsing() {
    InFluxTelegrafConsumer.TelegrafMeasure measure = consumer.parse("net_drop_out,best=teemu,tom=teemun\\ kone,interface=en0 value=0i 1434055562000000000");
    assertEquals(measure.name, "net_drop_out", "Parsed measurement name");
    assertEquals(measure.fieldName, "value", "Parsed measurement field name");
    assertEquals(measure.fieldValue, "0i", "Parsed measurement field value");
    assertEquals(measure.time, 1434055562000000000L, "Parsed measurement time");
    assertEquals(measure.tags.size(), 3, "Parsed measurement tag count");
    assertEquals(measure.tags.get("best"), "teemu", "Teemu should be best (tag)");
    assertEquals(measure.tags.get("tom"), "teemun_kone", "'tom' tag in measurement");
    assertEquals(measure.tags.get("interface"), "en0", "Interface tag");
  }

  @Test
  public void tomMissing() {

  }
}

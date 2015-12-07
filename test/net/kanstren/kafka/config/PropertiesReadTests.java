package net.kanstren.kafka.config;

import org.testng.annotations.Test;
import osmo.common.TestUtils;
import net.kanstren.kafka.influx.Config;
import net.kanstren.kafka.influx.Main;

import java.io.StringReader;
import java.util.Properties;

import static org.testng.Assert.assertEquals;

/**
 * @author Teemu Kanstren.
 */
public class PropertiesReadTests {
  @Test
  public void readTopics() throws Exception {
    Properties props = new Properties();
    props.load(new StringReader(TestUtils.getResource(PropertiesReadTests.class, "test.properties")));
    //this throws an error if the properties are not properly initialized
    Main.init(props);
    assertEquals(Config.kafkaAvroTopics.size(), 3, "Number of parsed Avro topics and db's");
    assertEquals(Config.kafkaJsonTopics.size(), 1, "Number of parsed JSON topics and db's");
    assertEquals(Config.kafkaTelegrafTopics.size(), 1, "Number of parsed Telegraf topics and db's");
    assertEquals(Config.kafkaAvroTopics.get("router_avro"), "router", "Parsed Avro topic");
    assertEquals(Config.kafkaAvroTopics.get("tester_avro"), "tester", "Parsed Avro topic");
    assertEquals(Config.kafkaAvroTopics.get("browser_avro"), "browser", "Parsed Avro topic");
    assertEquals(Config.kafkaJsonTopics.get("router_json"), "router", "Parsed JSON topic");
    assertEquals(Config.kafkaTelegrafTopics.get("telegraf"), "telegraf", "Parsed Telegraf topic");
  }

  @Test
  public void missingPostFix() throws Exception {
    Properties props = new Properties();
    props.load(new StringReader(TestUtils.getResource(PropertiesReadTests.class, "test_nopostfix.properties")));
    //this throws an error if the properties are not properly initialized
    try {
      Main.init(props);
    } catch (Exception e) {
      assertEquals(e.getMessage(), "Missing property 'db_postfix' in configuration file kafka-importer.properties.\n", "Missing postfix message");
    }
  }
}

package net.kanstren.kafka.cassandra;

import org.apache.avro.Schema;
import org.testng.annotations.Test;
import osmo.common.TestUtils;
import net.kanstren.kafka.cassanda.CassandaAvroConsumer;
import net.kanstren.kafka.influx.Config;
import net.kanstren.kafka.influx.avro.SchemaRepository;

import java.util.Collection;

import static org.testng.Assert.assertEquals;

/**
 * @author Teemu Kanstren.
 */
public class CassandraAvroTests {
//  @Test
//  public void repoToTables() {
//    Config.cassandraKeySpace = "test_ks";
//    SchemaRepository repo = new SchemaRepository();
//    Collection<Schema> schemas = repo.getSchemas();
//    String tables = CassandaAvroConsumer.tables(repo);
//    String expected = TestUtils.getResource(CassandraAvroTests.class, "expected_repo_tables.txt");
//    assertEquals(tables, expected, "Generated table for schema repo");
//  }

  @Test
  public void schemaSNMPFloatToTable() {
    Config.cassandraKeySpace = "test_ks";
    SchemaRepository repo = new SchemaRepository();
    Collection<Schema> schemas = repo.getSchemas();
    Schema snmpFloat = null;
    for (Schema schema : schemas) {
      String name = schema.getName();
      if (name.equals("SNMPFloat")) {
        snmpFloat = schema;
        break;
      }
    }
    String table = CassandaAvroConsumer.tableFor(snmpFloat);
    String expected = TestUtils.getResource(CassandraAvroTests.class, "expected_snmpfloat_table.txt");
    assertEquals(table, expected, "Generated table for SNMPFloat schema");
  }

  @Test
  public void schemaAvgToTable() {
    Config.cassandraKeySpace = "test_ks";
    SchemaRepository repo = new SchemaRepository();
    Collection<Schema> schemas = repo.getSchemas();
    Schema snmpFloat = null;
    for (Schema schema : schemas) {
      String name = schema.getName();
      if (name.equals("Averages")) {
        snmpFloat = schema;
        break;
      }
    }
    String table = CassandaAvroConsumer.tableFor(snmpFloat);
    String expected = TestUtils.getResource(CassandraAvroTests.class, "expected_avg_table.txt");
    assertEquals(table, expected, "Generated table for Averages schema");
  }

  @Test
  public void insertForSNMPFloat() {
    Config.cassandraKeySpace = "test_ks";
    SchemaRepository repo = new SchemaRepository();
    Collection<Schema> schemas = repo.getSchemas();
    Schema snmpFloat = null;
    for (Schema schema : schemas) {
      String name = schema.getName();
      if (name.equals("SNMPFloat")) {
        snmpFloat = schema;
        break;
      }
    }
    String table = CassandaAvroConsumer.insertFor(snmpFloat);
    String expected = TestUtils.getResource(CassandraAvroTests.class, "expected_snmpfloat_insert.txt");
    assertEquals(table, expected, "Generated insert for SNMPFloat schema");
  }

  @Test
  public void insertForAverage() {
    Config.cassandraKeySpace = "test_ks";
    SchemaRepository repo = new SchemaRepository();
    Collection<Schema> schemas = repo.getSchemas();
    Schema snmpFloat = null;
    for (Schema schema : schemas) {
      String name = schema.getName();
      if (name.equals("Averages")) {
        snmpFloat = schema;
        break;
      }
    }
    String table = CassandaAvroConsumer.insertFor(snmpFloat);
    String expected = TestUtils.getResource(CassandraAvroTests.class, "expected_avg_insert.txt");
    assertEquals(table, expected, "Generated insert for Averages schema");
  }
}

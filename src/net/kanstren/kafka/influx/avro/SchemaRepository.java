package net.kanstren.kafka.influx.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import osmo.common.TestUtils;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;

/**
 * An Avro schema repository. Used to find schemas for decoding binary encoded messages.
 * Works by reading the schema id values and matching schema names from a configuration file called 'schema_id.proeprties'.
 * These are then mapped to Avro schema definitions that are assumed to be found in the 'schemas' directory.
 *
 * @author Teemu Kanstren.
 */
public class SchemaRepository {
  /** Key = schema id, value = schema itself. */
  private Map<Integer, Schema> schemas = new HashMap<>();
  /** Key = schema id, value = Avro reader for the schema. */
  private Map<Integer, GenericDatumReader<GenericRecord>> readers = new HashMap<>();

  public SchemaRepository() {
    List<String> files = TestUtils.listFiles("schemas", "avsc", false);
    Properties config = new Properties();
    try {
      config.load(new FileInputStream("schema_ids.properties"));
    } catch (IOException e) {
      throw new RuntimeException("Unable to read schema id file 'schema_id.properties' in working directory.", e);
    }
    Set<String> ids = config.stringPropertyNames();
    String errors = "";
    for (String idName : ids) {
      if (!files.contains(idName)) {
        errors += "Unable to find schema for which an ID has been defined:"+idName+". Is this in your 'schema' directory (under working dir)?\n";
      } else {
        Schema schema = new Schema.Parser().parse(TestUtils.readFile("schemas/"+idName, "UTF8"));
        GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
        String idStr = config.getProperty(idName);
        try {
          int id = Integer.parseInt(idStr);
          schemas.put(id, schema);
          readers.put(id, reader);
        } catch (NumberFormatException e) {
          errors += "Unable to parse schema id value for '"+idName+"'. Invalid integer: "+idStr;
        }
      }
    }
    if (errors.length() > 0) throw new IllegalArgumentException(errors);
  }

  public Schema schemaFor(int id) {
    return schemas.get(id);
  }

  public GenericDatumReader<GenericRecord> readerFor(int id) {
    return readers.get(id);
  }

  public Collection<Schema> getSchemas() {
    return schemas.values();
  }

  public Map<Integer, Schema> getSchemaMap() {
    return schemas;
  }
}

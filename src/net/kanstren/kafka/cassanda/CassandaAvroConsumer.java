package net.kanstren.kafka.cassanda;

import com.datastax.driver.core.*;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import net.kanstren.kafka.influx.Config;
import net.kanstren.kafka.influx.avro.SchemaRepository;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.util.Utf8;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.*;

/**
 * Kafka consumer listening to measurement data.
 * Reads the Kafka topic stream and stores received data to the configured Cassandra instance.
 *
 * @author Teemu Kanstren.
 */
public class CassandaAvroConsumer implements Runnable {
  private static final Logger log = LogManager.getLogger();
  /** The Kafka measurement data stream. */
  private final KafkaStream stream;
  /** Identifier for the thread this consumer is running on. */
  private final int id;
  /** Cassandra cluster. */
  private final Cluster cluster;
  /** Cassandra session. */
  private final Session session;
  /** To create unique thread id values. */
  private static int nextId = 1;
  /** Repository for Avro schemas. Needed to decode binary messages. */
  private final SchemaRepository repo;
  /** Number of messages processed. */
  private int count = 0;
  /** Cassandra insert statements for the Avro schemas. Key = schema id, value = insert statement. */
  private Map<Integer, PreparedStatement> insertStatements = new HashMap<>();

  public CassandaAvroConsumer(SchemaRepository repo, KafkaStream stream) {
    this.repo = repo;
    this.stream = stream;
    this.id = nextId++;

    this.cluster = Cluster.builder()
            .addContactPoint(Config.cassandraUrl)
            .build();
    Metadata metadata = cluster.getMetadata();
    log.debug("Connected to cluster: %s\n", metadata.getClusterName());
    for (Host host : metadata.getAllHosts()) {
      log.debug("Datatacenter: %s; Host: %s; Rack: %s\n", host.getDatacenter(), host.getAddress(), host.getRack());
    }
    this.session = cluster.connect();
    createSchemas();
    createInserts();
  }

  /**
   * Create Cassandra tables for the Avro schemas (as needed).
   */
  public void createSchemas() {
    String stmt = "CREATE KEYSPACE IF NOT EXISTS " + Config.cassandraKeySpace +
            " WITH replication = {'class':'SimpleStrategy', 'replication_factor':" + Config.cassandraReplicationFactor + "};";
    log.debug("Creating Cassandra keyspace:"+stmt);
    session.execute(stmt);
    createTables(repo);
  }

  /**
   * Create Cassandra tables for Avro schemas as needed.
   *
   * @param repo The schemas to create tables for.
   */
  private void createTables(SchemaRepository repo) {
    Collection<Schema> schemas = repo.getSchemas();
    for (Schema schema : schemas) {
      String table = tableFor(schema);
      log.debug("Creating Cassandra tables:"+table);
      session.execute(table);
    }
  }

  /**
   * Create a Cassandra CREATE TABLE statement for a given Avro schema.
   * Primary key is always (time + type) combination.
   * These are assumed to be unique and give good spread across the cluster.
   *
   * @param schema The schema to generate the CREATE TABLE for.
   * @return The generated CREATE TABLE statement.
   */
  public static String tableFor(Schema schema) {
    Schema.Field headerField = schema.getField("header");
    Schema headerSchema = headerField.schema();
    List<Schema.Field> headerFields = headerSchema.getFields();
    String table = "CREATE TABLE IF NOT EXISTS "+Config.cassandraKeySpace+"."+toTableName(schema)+" (\n";
    table += "time timestamp,\n";
    for (Schema.Field field : headerFields) {
      String tagName = field.name();
      if (tagName.equals("time")) continue;
      table += columnFor(tagName, field.schema())+",\n";
    }

    Schema.Field bodyField = schema.getField("body");
    Schema bodySchema = bodyField.schema();
    List<Schema.Field> bodyFields = bodySchema.getFields();
    for (Schema.Field field : bodyFields) {
      String fieldName = field.name();
      table += columnFor(fieldName, field.schema())+",\n";
    }
    table += "PRIMARY KEY (time, type)\n";
//    table = table.substring(0, table.length()-2);
    table += ");";
    return table;
  }

  /**
   * Create a Cassandra column for the CREATE TABLE statement for a given field in the Avro schema.
   *
   * @param fieldName The name of the field to generate the column string for.
   * @param schema The schema where the given field belongs. Used to get field type.
   * @return A string to put in a CREATE TABLE statement for the field in the schema.
   */
  private static String columnFor(String fieldName, Schema schema) {
    String str = fieldName+" ";
    switch (schema.getType()) {
      case DOUBLE:
        str += "double";
        break;
      case FLOAT:
        str += "float";
        break;
      case INT:
        str += "int";
        break;
      case LONG:
        str += "bigint";
        break;
      case STRING:
        str += "text";
        break;
      case UNION:
        List<Schema> types = schema.getTypes();
        for (Schema option : types) {
          Schema.Type type = option.getType();
          if (type.equals(Schema.Type.NULL)) continue;
          return columnFor(fieldName, option);
        }
        break;
      default:
        throw new IllegalArgumentException("Unsupported field type:" + schema.getType());
    }
    return str;
  }

  /**
   * Create the insert statements for the Avro schemas.
   */
  private void createInserts() {
    Map<Integer, Schema> schemaMap = repo.getSchemaMap();
    schemaMap.forEach((id, schema) -> {
      String statement = insertFor(schema);
      PreparedStatement prepared = session.prepare(statement);
      insertStatements.put(id, prepared);
    });
  }

  /**
   * Create a Cassandra insert statement for a specific Avro schema.
   *
   * @param schema The Avro schema to create the insert for.
   * @return The insert statement to insert the type of Avro object into Cassandra.
   */
  public static String insertFor(Schema schema) {
    Schema.Field headerField = schema.getField("header");
    Schema headerSchema = headerField.schema();
    List<Schema.Field> headerFields = headerSchema.getFields();
    String columns = "time";
    int count = 0;
    for (Schema.Field field : headerFields) {
      String tagName = field.name();
      if (tagName.equals("time")) continue;
      columns += ", "+tagName;
      count++;
    }

    Schema.Field bodyField = schema.getField("body");
    Schema bodySchema = bodyField.schema();
    List<Schema.Field> bodyFields = bodySchema.getFields();
    for (Schema.Field field : bodyFields) {
      String name = field.name();
      columns += ", "+name;
      count++;
    }
    String values = "?";
    for (int i = 0 ; i < count ; i++) {
      values += ", ?";
    }
    String cql = "INSERT INTO "+Config.cassandraKeySpace+"."+toTableName(schema)+" " +
            "("+columns+") VALUES (" + values + ");";
    return cql;
  }

  private static String toTableName(Schema schema) {
    return schema.getName().toLowerCase();
  }

  @Override
  public void run() {
    try {
      runrun();
    } catch (Exception e) {
      log.error("Cassandra Avro consumer crash", e);
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
      if (msg.length < 2) {
        log.info("ignoring short msg, assuming topic polling");
        continue;
      }
      log.trace("Cassandra Avro Thread " + id + ":: " + Arrays.toString(msg));
      process(msg);
    }
    log.info("Shutting down Cassandra Avro consumer Thread: " + id);
  }

  /**
   * Process a binary message from Kafka.
   *
   * @param msg The Kafka message.
   */
  public void process(byte[] msg) {
    //always starts with the Avro schema id for decoding
    byte schemaId = msg[0];
    //get the matching schema to decode with
    Schema schema = repo.schemaFor(schemaId);
    GenericDatumReader<GenericRecord> reader = repo.readerFor(schemaId);
    //TODO: test for handling of invalid id values (not in repo)
    Schema.Field headerField = schema.getField("header");
    Schema headerSchema = headerField.schema();
    List<Schema.Field> headerFields = headerSchema.getFields();
    Schema.Field bodyField = schema.getField("body");
    Schema bodySchema = bodyField.schema();
    List<Schema.Field> bodyFields = bodySchema.getFields();

    Decoder d = DecoderFactory.get().binaryDecoder(msg, 1, msg.length - 1, null);
    try {
      GenericRecord record = reader.read(null, d);
      GenericRecord header = (GenericRecord) record.get("header");
      GenericRecord body = (GenericRecord) record.get("body");

      //store the decoded message into Cassandra
      store(schemaId, header, body, headerFields, bodyFields);
//      log.trace("Stored msg:"+record);
    } catch (IOException e) {
      log.error("Error while processing received Kafka msg. Skipping this msg:" + Arrays.toString(msg), e);
    }
  }

  /**
   * Store the given Avro object into Cassandra.
   *
   * @param schemaId The Avro schema id.
   * @param header Header schema for the Avro object.
   * @param body Body schema for the Avro object.
   * @param headerFields Header fields for the Avro object.
   * @param bodyFields Body fields for the Avro object.
   */
  private void store(int schemaId, GenericRecord header, GenericRecord body, List<Schema.Field> headerFields, List<Schema.Field> bodyFields) {
    String type = header.get("type").toString();
    type = type.replace(' ', '_');
    //remove leading _ as it is currently used to represent multi-valued fields. relevant for influxdb not cassandra.
    //and this comment will surely be here when we manage to fix this req for influx-db and remove this check...
    if (type.startsWith("_")) type = type.substring(1);
    long time = (Long) header.get("time");

    //here we assume tables are always created with time as column 1, type as column 2
    List<Object> parameters = new ArrayList<>();
    parameters.add(new Date(time));
    parameters.add(type);

    //here we need to be careful to iterate everything in the same order as we did in createing the INSERT statements
    for (Schema.Field field : headerFields) {
      String name = field.name();
      Object value = header.get(name);
      if (name.equals("type") || name.equals("time")) continue;
//      if (value == null) continue;
      if (value instanceof Utf8) {
        value = value.toString();
      }
      parameters.add(value);
    }

    for (Schema.Field field : bodyFields) {
      String name = field.name();
      Object value = body.get(name);
      if (value instanceof Utf8) {
        value = value.toString();
      }
//      if (value == null) continue;
      parameters.add(value);
    }

    count++;

    //and then we stick it in Cassandra
    BoundStatement  bs = new BoundStatement(insertStatements.get(schemaId));
    bs.bind(parameters.toArray());
    session.execute(bs);
    log.debug("Executed:"+bs);

    if (count % 100 == 0) System.out.print(count + ",");
    if (count % 1000 == 0) System.out.println();
  }
}

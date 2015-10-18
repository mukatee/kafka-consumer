package osmo.monitoring.kafka.cassanda;

import com.datastax.driver.core.*;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.util.Utf8;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.influxdb.dto.Point;
import osmo.monitoring.kafka.influx.Config;
import osmo.monitoring.kafka.influx.Main;
import osmo.monitoring.kafka.influx.avro.SchemaRepository;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Kafka consumer listening to measurement data.
 * Reads the Kafka topic stream and stores received data to the configured InfluxDB instance.
 *
 * @author Teemu Kanstren.
 */
public class CassandaAvroConsumer implements Runnable {
  private static final Logger log = LogManager.getLogger();
  /** The Kafka measurement data stream. */
  private final KafkaStream stream;
  /** Identifier for the thread this consumer is running on. */
  private final int id;
  private final Cluster cluster;
  private final Session session;
  /** To create unique thread id values. */
  private static int nextId = 1;
  private final SchemaRepository repo;
  private int count = 0;
  private Map<Integer, PreparedStatement> insertStatements = new HashMap<>();

  public CassandaAvroConsumer(SchemaRepository repo, KafkaStream stream) {
    this.repo = repo;
    this.stream = stream;
    this.id = nextId++;

    this.cluster = Cluster.builder()
            .addContactPoint(Config.cassandraUrl)
            .build();
    Metadata metadata = cluster.getMetadata();
    System.out.printf("Connected to cluster: %s\n", metadata.getClusterName());
    for (Host host : metadata.getAllHosts()) {
      System.out.printf("Datatacenter: %s; Host: %s; Rack: %s\n", host.getDatacenter(), host.getAddress(), host.getRack());
    }
    this.session = cluster.connect();
    createSchemas();
    createInserts();
  }

  public static void main(String[] args) throws Exception {
    Main.init();
    CassandaAvroConsumer cassandra = new CassandaAvroConsumer(new SchemaRepository(), null);
  }

  public void createSchemas() {
    String stmt = "CREATE KEYSPACE IF NOT EXISTS " + Config.cassandraKeySpace +
            " WITH replication = {'class':'SimpleStrategy', 'replication_factor':" + Config.cassandraReplicationFactor + "};";
    log.debug("Createing Cassandra keyspace:"+stmt);
    session.execute(stmt);
    createTables(repo);
  }

  private void createTables(SchemaRepository repo) {
    Collection<Schema> schemas = repo.getSchemas();
    for (Schema schema : schemas) {
      String table = tableFor(schema);
      log.debug("Creating Cassandra tables:"+table);
      session.execute(table);
    }
  }

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

  private void createInserts() {
    Map<Integer, Schema> schemaMap = repo.getSchemaMap();
    schemaMap.forEach((id, schema) -> {
      String statement = insertFor(schema);
      PreparedStatement prepared = session.prepare(statement);
      insertStatements.put(id, prepared);
    });
  }

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

  public void process(byte[] msg) {
    byte schemaId = msg[0];
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

      store(schemaId, header, body, headerFields, bodyFields);
//      log.trace("Stored msg:"+record);
    } catch (IOException e) {
      log.error("Error while processing received Kafka msg. Skipping this msg:" + Arrays.toString(msg), e);
    }
  }

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

    BoundStatement  bs = new BoundStatement(insertStatements.get(schemaId));
    bs.bind(parameters.toArray());
    session.execute(bs);
    log.debug("Executed:"+bs);

    if (count % 100 == 0) System.out.print(count + ",");
    if (count % 1000 == 0) System.out.println();
  }
}

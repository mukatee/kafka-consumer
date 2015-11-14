package net.kanstren.kafka.influx;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Provides a global repository of InfluxDB access interfaces for different database instances.
 *
 * @author Teemu Kanstren.
 */
public class InFlux {
  /** Key = database name, Value = Database access interface. */
  private static final Map<String, InfluxDB> influxes = new HashMap<>();

  /**
   * Get the InfluxDB instance for given database name.
   *
   * @param dbName The database name to get.
   * @return Interface to access the database.
   */
  public synchronized static InfluxDB influxFor(String dbName) {
    InfluxDB db = influxes.get(dbName);
    if (db != null) return db;
    db = InfluxDBFactory.connect(Config.influxDbUrl, Config.influxUser, Config.influxPass);
    db.enableBatch(2000, 1, TimeUnit.SECONDS);
//    db.setLogLevel(InfluxDB.LogLevel.HEADERS);
    db.createDatabase(dbName);
    influxes.put(dbName, db);
    return db;
  }
}

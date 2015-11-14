# Kafka-Consumer

Reads data from Kafka topics and stores to InfluxDB. Also draft support for Cassandra but still rather limited.

To use, set the configuration parameters in kafka-importer.properties file. Use the kafka-importer.properties.example as a template.

The data is read in either Avro or Telegraf format from Kafka topic streams.
There is also some support for JSON but I have not used it for a while so it is probably outdated.

##Avro

When using Avro, the schemas for the Avro objects passed over Kafka need to be made available.
The schema files should be put in the working directory under "schemas".
The id values for these schemas need to be defined in "schema_ids.properties" file.
The data is expected to be passed over Kafka with the first byte indicating the schema ID value, and the following bytes
to contain the actual Avro object.
The schema id is then used to locate the schema to decode the actual binary data into Avro.
This is then stored into the configured InfluxDB instance.

The Avro objects are expected to have the following general format for parsing:

- Header field:
  - Time: Measurement timestamp.
  - Type: Measurement type.
  - Other: Any other fields in the header are stored as metadata tags in InfluxDB. The name is taken from the schema for the field and value from object.
- Body field:
  - Any: Any fields in the body are taken as "fields" in InfluxDB and stored as such. Name is taken from schema and value from object.

There are examples under the "schema" directory.
One example producer is [PyPro](http://github.com/mukatee/pypro), which can send SNMP measures over Kafka for this consumer.

##Telegraf

The [Telegraf](https://github.com/influxdb/telegraf) consumer reads data from the Kafka topic in InfluxDB line protocol format
and stores this into the configured InfluxDB database.
Telegraf comes with its own Kafka producer than can be used for this.

If you are only interested in dumping Telegraf data into InfluxDB you are probably better off using its direct support to write into InfluxDB.
In this case, I have used Kafka to stream data to many targets such as InfluxDB, Storm, Spark, Cassandra, which is why this parsing is done here as well.

The Telegraf version used here was 0.2 so later ones might require tweaking the protocol parser if that changes.
Also at this time the Telegraf timestamps had some issues so the time is taken from the time this component received the measurements.
This needs to be changed once Telegraf is fixed.

##Use

The main class is net.kanstren.kafka.influx.Main. You currently need to get the repo, compile and run if you want to use it.
If there is some interest in using this more generally I might do a Maven deployment.

LICENSE: The MIT license.
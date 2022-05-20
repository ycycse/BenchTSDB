package cn.edu.thu.database.timescaledb;

import cn.edu.thu.common.Config;
import cn.edu.thu.common.Record;
import cn.edu.thu.common.Schema;
import cn.edu.thu.database.IDataBaseManager;
import cn.edu.thu.database.influxdb.InfluxDBManager;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TimescaleDBManager implements IDataBaseManager {

  private static Logger logger = LoggerFactory.getLogger(InfluxDBManager.class);
  private static final String defaultDatabase = "postgres";
  private Config config;

  private static final String POSTGRESQL_JDBC_NAME = "org.postgresql.Driver";
  private static final String POSTGRESQL_URL = "jdbc:postgresql://%s:%s/%s";

  private static final String tableName = "tb1";
  private boolean tableCreated = false;

  // chunk_time_interval=7d
  private static final String CONVERT_TO_HYPERTABLE =
      "SELECT create_hypertable('%s', 'time', chunk_time_interval => 604800000);";
  private static final String dropTable = "DROP TABLE %s;";

  private Connection connection;

  private static String COUNT_SQL_WITH_TIME = "select count(%s) from %s where time >= %dms and time <= %dms and %s='%s'";

  private static String COUNT_SQL_WITHOUT_TIME = "select count(%s) from %s where %s ='%s'";


  public TimescaleDBManager(Config config) {
    this.config = config;
  }

  @Override
  public void initServer() {
    try {
      Class.forName(POSTGRESQL_JDBC_NAME);
      // first clear if exist, and create database
      String defaultUrl = String
          .format(POSTGRESQL_URL, config.TIMESCALEDB_HOST, config.TIMESCALEDB_PORT,
              defaultDatabase);
      logger.info("connecting url: " + defaultUrl);
      connection = DriverManager.getConnection(defaultUrl, config.TIMESCALEDB_USERNAME,
          config.TIMESCALEDB_PASSWORD);
      Statement statement = connection.createStatement();
      statement.execute(String.format("DROP DATABASE IF EXISTS %s;", config.TIMESCALEDB_DATABASE));
      statement.execute(String.format("CREATE database %s;", config.TIMESCALEDB_DATABASE));
      statement.close();
      connection.close();
      // next set up the TimescaleDB extension for the database
      // https://docs.timescale.com/install/latest/self-hosted/installation-windows/#set-up-the-timescaledb-extension
      String url = String.format(POSTGRESQL_URL, config.TIMESCALEDB_HOST, config.TIMESCALEDB_PORT,
          config.TIMESCALEDB_DATABASE);
      logger.info("connecting url: " + url);
      connection = DriverManager.getConnection(url, config.TIMESCALEDB_USERNAME,
          config.TIMESCALEDB_PASSWORD);
      statement = connection.createStatement();
      statement.execute("CREATE EXTENSION IF NOT EXISTS timescaledb;");
      statement.close();
      connection.close();
    } catch (Exception e) {
      logger.error("Initialize TimescaleDB failed because ", e);
    }
  }

  @Override
  public void initClient() {
    try {
      Class.forName(POSTGRESQL_JDBC_NAME);
      String url = String.format(POSTGRESQL_URL, config.TIMESCALEDB_HOST, config.TIMESCALEDB_PORT,
          config.TIMESCALEDB_DATABASE);
      logger.info("connecting url: " + url);
      connection = DriverManager.getConnection(url, config.TIMESCALEDB_USERNAME,
          config.TIMESCALEDB_PASSWORD);
    } catch (Exception e) {
      logger.error("Initialize TimescaleDB failed because ", e);
    }
  }

  @Override
  public long count(String tagValue, String field, long startTime, long endTime) {

//    String sql;
//
//    if (startTime == -1 || endTime == -1) {
//      sql = String.format(COUNT_SQL_WITHOUT_TIME, field, measurementId, Config.TAG_NAME, tagValue);
//    } else {
//      sql = String
//          .format(COUNT_SQL_WITH_TIME, field, measurementId, startTime, endTime, Config.TAG_NAME,
//              tagValue);
//    }
//
//    logger.info("Executing sql {}", sql);

    long start = System.nanoTime();

//    QueryResult queryResult = influxDB.query(new Query(sql, database));

//    logger.info(queryResult.toString());

    return System.nanoTime() - start;

  }

  @Override
  public long flush() {
    return 0;
  }

  @Override
  public long close() {
    if (connection == null) {
      return 0;
    }
    try {
      connection.close();
    } catch (Exception e) {
      logger.error("Failed to close TimeScaleDB connection because: {}", e.getMessage());
    }
    return 0;
  }

  private void registerSchema(Schema schema) {
    try (Statement statement = connection.createStatement()) {
      String pgsql = getCreateTableSql(tableName, schema);
      logger.info("CreateTableSQL Statement:  {}", pgsql);
      statement.execute(pgsql);
      logger.debug(
          "CONVERT_TO_HYPERTABLE Statement:  {}", String.format(CONVERT_TO_HYPERTABLE, tableName));
      statement.execute(String.format(CONVERT_TO_HYPERTABLE, tableName));
    } catch (Exception e) {
      logger.error("Can't create PG table because: {}", e.getMessage());
    }
  }

  private String getCreateTableSql(String tableName, Schema schema) {
    StringBuilder sqlBuilder = new StringBuilder("CREATE TABLE ").append(tableName).append(" (");
    sqlBuilder.append("time BIGINT NOT NULL, device TEXT NOT NULL");
//    for (Map.Entry<String, String> pair : config.getDEVICE_TAGS().entrySet()) {
//      sqlBuilder.append(", ").append(pair.getKey()).append(" TEXT NOT NULL");
//    }
    for (int i = 0; i < schema.getFields().length; i++) {
      sqlBuilder
          .append(", ")
          .append(transformColumnName(schema.getFields()[i]))
          .append(" ")
          .append(typeMap(schema.getTypes()[i]))
          .append(" NULL ");
    }
//    sqlBuilder.append(",UNIQUE (time, sGroup, device");
//    for (Map.Entry<String, String> pair : config.getDEVICE_TAGS().entrySet()) {
//      sqlBuilder.append(", ").append(pair.getKey());
//    }
    sqlBuilder.append(");");
    return sqlBuilder.toString();
  }

  private String typeMap(Class<?> sensorType) {
    if (sensorType == Long.class) {
      return "BIGINT";
    } else if (sensorType == Double.class) {
      return "DOUBLE PRECISION";
    } else if (sensorType == String.class) {
      return "TEXT";
    } else {
      logger.error(
          "Unsupported data sensorType {}, use default data sensorType: BINARY.", sensorType);
      return "TEXT";
    }
  }

  @Override
  public long insertBatch(List<Record> records, Schema schema) {
    if (!tableCreated) {
      registerSchema(schema);
      tableCreated = true;
    }

    long start = 0;
    try (Statement statement = connection.createStatement()) {
      for (Record record : records) {
        String sql = getInsertOneBatchSql(schema, record.timestamp, record.fields);
        statement.addBatch(sql);
        logger.debug(sql);
      }
      start = System.nanoTime();
      statement.executeBatch();
    } catch (Exception e) {
      e.printStackTrace();
      logger.error("meet error when writing: {}", e.getMessage());
    }
    return System.nanoTime() - start;
  }

  private String transformColumnName(String name) {
    return "\"" + name + "\""; // to escape special characters like -,@
  }

  /**
   * eg.
   *
   * <p>INSERT INTO conditions(time, group, device, s_0, s_1) VALUES (1535558400000, 'group_0',
   * 'd_0', 70.0, 50.0);
   */
  private String getInsertOneBatchSql(Schema schema, long timestamp, List<Object> values) {
    StringBuilder builder = new StringBuilder();
    builder.append("insert into ").append(tableName).append("(time,device");
    for (String sensor : schema.getFields()) {
      builder.append(",").append(transformColumnName(sensor));
    }
    builder.append(") values(");
    builder.append(timestamp);
    builder.append(",'").append(schema.getTag()).append("'");
    for (int i = 0; i < values.size(); i++) {
      Object value = values.get(i);
      if (schema.getTypes()[i] == String.class && value != null) {
        builder.append(",'").append(value).append("'");
      } else {
        builder.append(",").append(value);
      }
    }
    builder.append(");");
//    builder.append(") ON CONFLICT(time,sGroup,device");
//    for (Map.Entry<String, String> pair : deviceSchema.getTags().entrySet()) {
//      builder.append(", ").append(pair.getKey());
//    }
//    builder.append(") DO UPDATE SET ");
//    builder.append(sensors.get(0).getName()).append("=excluded.").append(sensors.get(0).getName());
//    for (int i = 1; i < sensors.size(); i++) {
//      builder
//          .append(",")
//          .append(sensors.get(i).getName())
//          .append("=excluded.")
//          .append(sensors.get(i).getName());
//    }
//    if (!config.isIS_QUIET_MODE()) {
//      LOGGER.debug("getInsertOneBatchSql: {}", builder);
//    }
    return builder.toString();
  }
}

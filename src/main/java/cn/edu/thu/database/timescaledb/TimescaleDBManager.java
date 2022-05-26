package cn.edu.thu.database.timescaledb;

import cn.edu.thu.common.Config;
import cn.edu.thu.common.Record;
import cn.edu.thu.common.Schema;
import cn.edu.thu.database.IDataBaseManager;
import cn.edu.thu.database.influxdb.InfluxDBManager;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
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

  //  private static final String tableName = "tb1";
  private static String currentTagTable = null;

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
      logger.info(
          "Done: " + String.format("DROP DATABASE IF EXISTS %s;", config.TIMESCALEDB_DATABASE));
      statement.execute(String.format("CREATE database %s;", config.TIMESCALEDB_DATABASE));
      logger.info("Done: " + String.format("CREATE database %s;", config.TIMESCALEDB_DATABASE));
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
  public long query() {
    String[] queryInfo = generateQuery();
    String queryURL = queryInfo[0];
    String sql = queryInfo[1];
    try {
      connection = DriverManager.getConnection(queryURL, config.TIMESCALEDB_USERNAME,
          config.TIMESCALEDB_PASSWORD);
    } catch (Exception e) {
      logger.error("Initialize TimescaleDB failed because ", e);
    }
    logger.info("connecting url: " + queryURL);
    logger.info("Begin query: {}", sql);

    long start = 0;
    long elapsedTime = 0;
    int c = 0;
    try (Statement statement = connection.createStatement()) {
      if (!config.QUERY_RESULT_PRINT_FOR_DEBUG) {
        start = System.nanoTime();
        ResultSet rs = statement.executeQuery(sql);
        while (rs.next()) {
          c++;
          for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
            // NOTE: Comparatively, IoTDB includes dataSet.next(). So here the process of extracting records is also included:
            rs.getObject(i); // but will this step be skipped by compiler?
          }
        }
        elapsedTime = System.nanoTime() - start;
      } else {
        start = System.nanoTime();
        ResultSet rs = statement.executeQuery(sql);
        while (rs.next()) {
          c++;
          StringBuilder line = new StringBuilder();
          for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
            line.append(rs.getObject(i));
            line.append(",");
          }
          logger.info(line.toString());
        }
        elapsedTime = System.nanoTime() - start;
      }
    } catch (Exception e) {
      e.printStackTrace();
      logger.error("meet error when writing: {}", e.getMessage());
    }
    logger.info("Query finished. Total lines: {}. SQL: {}", c, sql);
    return elapsedTime;
  }

  /**
   * @return the first string denotes queryURL, the second string denotes sql
   */
  private String[] generateQuery() {
    String queryURL = null;
    String sql = null;
    switch (config.QUERY_TYPE) {
      case "SINGLE_SERIES_RAW_QUERY":
        // use yanchang dataset
        queryURL = String
            .format(POSTGRESQL_URL, config.TIMESCALEDB_HOST, config.TIMESCALEDB_PORT, "yanchang");
        sql = String.format("select %s from %s limit %d;", encapName("collecttime"),
            encapName("root.T000100010002.90003"), config.QUERY_PARAM);
        break;
      case "MULTI_SERIES_ALIGN_QUERY":
        // use dianchang dataset
        queryURL = String
            .format(POSTGRESQL_URL, config.TIMESCALEDB_HOST, config.TIMESCALEDB_PORT, "dianchang");
        String sql_format = "select %s from %s;";
        StringBuilder selectSensors = new StringBuilder();
        for (int i = 1; i < config.QUERY_PARAM + 1; i++) {
          selectSensors.append(encapName("sensor" + i));
          if (i < config.QUERY_PARAM) {
            selectSensors.append(",");
          }
        }
        sql = String.format(sql_format, selectSensors.toString(), encapName("root.DianChang.d1"));
        break;
      case "SINGLE_SERIES_COUNT_QUERY":
        // use yanchang dataset
        queryURL = String
            .format(POSTGRESQL_URL, config.TIMESCALEDB_HOST, config.TIMESCALEDB_PORT, "yanchang");
        switch (config.QUERY_PARAM) {
          case 1:
            sql = String.format("select count(%s) from %s where time<=1601023212859;",
                encapName("collecttime"), encapName("root.T000100010002.90003"));
            break;
          case 100:
            sql = String.format("select count(%s) from %s where time<=1601023262692;",
                encapName("collecttime"), encapName("root.T000100010002.90003"));
            break;
          case 10000:
            sql = String.format("select count(%s) from %s where time<=1601045811969;",
                encapName("collecttime"), encapName("root.T000100010002.90003"));
            break;
          case 1000000:
            sql = String.format("select count(%s) from %s where time<=1602131946370;",
                encapName("collecttime"), encapName("root.T000100010002.90003"));
            break;
          default:
            logger.error("QUERY_PARAM not correct! Please check your configurations.");
            break;
        }
        break;
      case "SINGLE_SERIES_DOWNSAMPLING_QUERY":
        // use yanchang dataset
        queryURL = String
            .format(POSTGRESQL_URL, config.TIMESCALEDB_HOST, config.TIMESCALEDB_PORT, "yanchang");
        String sqlFormat =
            "select floor((time-%3$d)/%5$d)*%5$d+%3$d,count(%1$s) from %2$s where time>=%3$d "
                + "and time<%4$d group by floor((time-%3$d)/%5$d);";
        switch (config.QUERY_PARAM) { // note that the startTime is modified to align with influxdb group by time style
          case 1:
            sql = String
                .format(sqlFormat, encapName("collecttime"), encapName("root.T000100010002.90003"),
                    1601023212859L, 1602479033308L, 1);
            break;
          case 100:
            sql = String
                .format(sqlFormat, encapName("collecttime"), encapName("root.T000100010002.90003"),
                    1601023212800L, 1602479033308L, 100);
            break;
          case 10000:
            sql = String
                .format(sqlFormat, encapName("collecttime"), encapName("root.T000100010002.90003"),
                    1601023210000L, 1602479033308L, 10000);
            break;
          case 1000000:
            sql = String
                .format(sqlFormat, encapName("collecttime"), encapName("root.T000100010002.90003"),
                    1601023000000L, 1602479033308L, 1000000);
            break;
          default:
            logger.error("QUERY_PARAM not correct! Please check your configurations.");
            break;
        }
        break;
      default:
        logger.error("QUERY_TYPE not correct! Please check your configurations.");
        break;
    }
    return new String[]{queryURL, sql};
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
      String pgsql = getCreateTableSql(schema);
      logger.info("CreateTableSQL Statement:  {}", pgsql);
      // Can't create PG table because: 错误: 表最多可以有 1600 个字段
      statement.execute(pgsql);
      String convertToHyperTable = String
          .format("SELECT create_hypertable('%s', 'time', chunk_time_interval => %d);",
              encapName(schema.getTag()), config.TIMESCALEDB_CHUNK_TIME_INTERVAL);
      logger.info("CONVERT_TO_HYPERTABLE Statement:  {}", convertToHyperTable);
      statement.execute(convertToHyperTable);
    } catch (Exception e) {
      logger.error("Can't create PG table because: {}", e.getMessage());
    }
  }

  private String getCreateTableSql(Schema schema) {
    StringBuilder sqlBuilder = new StringBuilder("CREATE TABLE ")
        .append(encapName(schema.getTag()))
        .append(" (");
    sqlBuilder
        .append("time BIGINT NOT NULL"); // TODO: 因为这个数据库要求先注册表格schema，所以不再所有设备一张表格了，而是一个设备一张表方便一些
//    sqlBuilder.append("time BIGINT NOT NULL, device TEXT NOT NULL");
//    for (Map.Entry<String, String> pair : config.getDEVICE_TAGS().entrySet()) {
//      sqlBuilder.append(", ").append(pair.getKey()).append(" TEXT NOT NULL");
//    }
    for (int i = 0; i < schema.getFields().length; i++) {
      sqlBuilder
          .append(", ")
          .append(encapName(schema.getFields()[i]))
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
    if (currentTagTable == null || !currentTagTable.equals(schema.getTag())) {
      registerSchema(schema);
      currentTagTable = schema.getTag();
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

  private String encapName(String name) {
    return "\"" + name + "\""; // to escape special characters like -,@,.
  }

  /**
   * eg.
   *
   * <p>INSERT INTO conditions(time, group, device, s_0, s_1) VALUES (1535558400000, 'group_0',
   * 'd_0', 70.0, 50.0);
   */
  private String getInsertOneBatchSql(Schema schema, long timestamp, List<Object> values) {
    StringBuilder builder = new StringBuilder();
    builder.append("insert into ").append(encapName(schema.getTag())).append("(time");
//    builder.append("insert into ").append(tableName).append("(time,device");
    for (String sensor : schema.getFields()) {
      builder.append(",").append(encapName(sensor));
    }
    builder.append(") values(");
    builder.append(timestamp);
//    builder.append(",'").append(schema.getTag()).append("'");
    for (int i = 0; i < values.size(); i++) {
      Object value = values.get(i);
      if (schema.getTypes()[i] == String.class && value != null) {
        builder.append(",'").append(value).append("'");
        // NOTE that quotes are removed during converting lines to records, so here need to be added for string type in TimescaleDB.
        // And if quotes are not removed during converting lines to records, 'abc' will be ''abc'' here, which is wrong format.
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

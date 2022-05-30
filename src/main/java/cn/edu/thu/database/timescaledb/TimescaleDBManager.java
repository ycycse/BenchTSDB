package cn.edu.thu.database.timescaledb;

import cn.edu.thu.common.Config;
import cn.edu.thu.common.Record;
import cn.edu.thu.common.Schema;
import cn.edu.thu.database.IDataBaseManager;
import com.google.common.collect.EvictingQueue;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Queue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TimescaleDBManager implements IDataBaseManager {

  private static Logger logger = LoggerFactory.getLogger(TimescaleDBManager.class);
  private static final String defaultDatabase = "postgres";
  private Config config;

  private static final String POSTGRESQL_JDBC_NAME = "org.postgresql.Driver";
  private static final String POSTGRESQL_URL = "jdbc:postgresql://%s:%s/%s";

  //  private static final String tableName = "tb1";
  private String currentTagTable = null; // NOTE: 不要设置为static，因为逻辑不可以被多个线程共同修改

  //  private static final int fieldLimit = 1599; // PG表最多可以有 1600 个字段，time占去一个
  private static final int fieldLimit = 700; // PG表最多可以有 1600 个字段，time占去一个。同时PG还有一行的大小限制，不能超过8160.

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
    String[] res = generateQuery();
    String queryURL = res[0];
    String[] sqls = Arrays.copyOfRange(res, 1, res.length);
    try {
      connection = DriverManager.getConnection(queryURL, config.TIMESCALEDB_USERNAME,
          config.TIMESCALEDB_PASSWORD);
    } catch (Exception e) {
      logger.error("Initialize TimescaleDB failed because ", e);
    }
    logger.info("connecting url: " + queryURL);
    for (String sql : sqls) {
      logger.info("Begin query: {}", sql);
    }

    long start = 0;
    long elapsedTime = 0;
    int c = 0;
    try (Statement statement = connection.createStatement()) {
      if (!config.QUERY_RESULT_PRINT_FOR_DEBUG) {
        // use queue to store results to avoid JIT compiler loop unrolling
        Queue<String> fifo = EvictingQueue.create(config.QUERY_RESULT_QUEUE_LINE_LIMIT);
        start = System.nanoTime();
        for (String sql : sqls) {
          ResultSet rs = statement.executeQuery(sql);
          while (rs.next()) {
            c++;
            StringBuilder line = new StringBuilder();
            for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
              // NOTE: Comparatively, IoTDB includes dataSet.next(). So here the process of extracting records is also included:
              line.append(rs.getObject(i));
              line.append(",");
            }
            fifo.add(line.toString());
          }
        }
        elapsedTime = System.nanoTime() - start;
        logger.info(fifo.toString());
      } else {
        start = System.nanoTime();
        for (String sql : sqls) {
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
        }
        elapsedTime = System.nanoTime() - start;
      }
    } catch (Exception e) {
      e.printStackTrace();
      logger.error("meet error when writing: {}", e.getMessage());
    }

    logger.info("Query finished. Total lines: {}. Query Number: {}", c, sqls.length);
    for (int n = 1; n <= sqls.length; n++) {
      logger.info("SQL{}: {}", n, sqls[n - 1]);
    }
    return elapsedTime;
  }

  /**
   * @return the first string denotes queryURL, the rest strings denote sqls
   */
  private String[] generateQuery() {
    String[] res = null;
    switch (config.QUERY_TYPE) {
      case "SINGLE_SERIES_RAW_QUERY":
        // use yanchang dataset
        String queryURL = String
            .format(POSTGRESQL_URL, config.TIMESCALEDB_HOST, config.TIMESCALEDB_PORT, "yanchang");
        res = new String[2];
        res[0] = queryURL;
        res[1] = String.format("select time,%s from %s limit %d;", encapName("collecttime"),
            encapName(tagOneToMore("root.T000100010002.90003", 1)),
            config.QUERY_PARAM); // "root.T000100010002.90003" sensor number is less than fieldLimit=700, so only one table is selected
        break;
      case "MULTI_SERIES_ALIGN_QUERY":
        // use dianchang dataset
        queryURL = String
            .format(POSTGRESQL_URL, config.TIMESCALEDB_HOST, config.TIMESCALEDB_PORT, "dianchang");
        int splitNum = (int) Math.ceil(config.QUERY_PARAM * 1.0 / fieldLimit);
        res = new String[1 + splitNum];
        res[0] = queryURL;
        for (int n = 1; n <= splitNum; n++) {
          String sql_format = "select time,%s from %s;";
          StringBuilder selectSensors = new StringBuilder();
          for (int i = (n - 1) * fieldLimit + 1;  // dianchang dataset has sensorID starting from 1
              i <= Math.min(config.QUERY_PARAM, n * fieldLimit); i++) {
            selectSensors.append(encapName("sensor" + i));
            if (i < Math.min(config.QUERY_PARAM, n * fieldLimit)) {
              selectSensors.append(",");
            }
          }
          res[n] = String
              .format(sql_format, selectSensors.toString(),
                  encapName(tagOneToMore("root.DianChang.d1", n)));
        }
        break;
      case "SINGLE_SERIES_COUNT_QUERY":
        // use yanchang dataset
        queryURL = String
            .format(POSTGRESQL_URL, config.TIMESCALEDB_HOST, config.TIMESCALEDB_PORT, "yanchang");
        res = new String[2];
        res[0] = queryURL;
        switch (config.QUERY_PARAM) {
          case 1:
            res[1] = String.format("select count(%s) from %s where time<=1601023212859;",
                encapName("collecttime"), encapName(tagOneToMore("root.T000100010002.90003", 1)));
            break;
          case 100:
            res[1] = String.format("select count(%s) from %s where time<=1601023262692;",
                encapName("collecttime"), encapName(tagOneToMore("root.T000100010002.90003", 1)));
            break;
          case 10000:
            res[1] = String.format("select count(%s) from %s where time<=1601045811969;",
                encapName("collecttime"), encapName(tagOneToMore("root.T000100010002.90003", 1)));
            break;
          case 1000000:
            res[1] = String.format("select count(%s) from %s where time<=1604742917425;",
                encapName("collecttime"), encapName(tagOneToMore("root.T000100010002.90003", 1)));
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
        res = new String[2];
        res[0] = queryURL;
        String sqlFormat =
            "select floor((time-%3$d)/%5$d)*%5$d+%3$d,count(%1$s) from %2$s where time>=%3$d "
                + "and time<%4$d group by floor((time-%3$d)/%5$d);";
        switch (config.QUERY_PARAM) { // note that the startTime is modified to align with influxdb group by time style
          case 1:
            res[1] = String
                .format(sqlFormat, encapName("collecttime"),
                    encapName(tagOneToMore("root.T000100010002.90003", 1)),
                    1601023212859L, 1602479033308L, 1);
            break;
          case 100:
            res[1] = String
                .format(sqlFormat, encapName("collecttime"),
                    encapName(tagOneToMore("root.T000100010002.90003", 1)),
                    1601023212800L, 1602479033308L, 100);
            break;
          case 10000:
            res[1] = String
                .format(sqlFormat, encapName("collecttime"),
                    encapName(tagOneToMore("root.T000100010002.90003", 1)),
                    1601023210000L, 1602479033308L, 10000);
            break;
          case 1000000:
            res[1] = String
                .format(sqlFormat, encapName("collecttime"),
                    encapName(tagOneToMore("root.T000100010002.90003", 1)),
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
    return res;
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
    // TODO: Can't create PG table because: 错误: 表最多可以有 1600 个字段
    try (Statement statement = connection.createStatement()) {
      String[] pgsql = getCreateTableSql(schema);
      logger.info("Create {} tables...", pgsql.length);
      for (int i = 1; i <= pgsql.length; i++) {
        String sql = pgsql[i - 1];
        logger.info("CreateTableSQL Statement:  {}", sql);
        statement.execute(sql);
        String convertToHyperTable = String
            .format("SELECT create_hypertable('%s', 'time', chunk_time_interval => %d);",
                encapName(tagOneToMore(schema.getTag(), i)), // note start from 1
                config.TIMESCALEDB_CHUNK_TIME_INTERVAL);
        logger.info("CONVERT_TO_HYPERTABLE Statement:  {}", convertToHyperTable);
        statement.execute(convertToHyperTable);
      }
    } catch (Exception e) {
      logger.error("Can't create PG table because: {}", e.getMessage());
    }
  }

  private String tagOneToMore(String tag, int n) {
    return tag + "_" + n;
  }

  private String[] getCreateTableSql(Schema schema) {
    // TODO: Can't create PG table because: 错误: 表最多可以有 1600 个字段
    int splitNum = (int) Math.ceil(schema.getFields().length * 1.0 / fieldLimit);
    String[] sqls = new String[splitNum];
    for (int n = 1; n <= splitNum; n++) {
      String tableName = encapName(tagOneToMore(schema.getTag(), n)); // note start from 1
      StringBuilder sqlBuilder = new StringBuilder("CREATE TABLE ")
          .append(tableName)
          .append(" (");
      sqlBuilder.append("time BIGINT NOT NULL");
      for (int i = (n - 1) * fieldLimit; i < Math.min(schema.getFields().length, n * fieldLimit);
          i++) {
        sqlBuilder
            .append(", ")
            .append(encapName(schema.getFields()[i]))
            .append(" ")
            .append(typeMap(schema.getTypes()[i]))
            .append(" NULL ");
      }
      sqlBuilder.append(");");
      sqls[n - 1] = sqlBuilder.toString();
    }
    return sqls;
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
    // TODO: Can't create PG table because: 错误: 表最多可以有 1600 个字段
    if (currentTagTable == null || !currentTagTable.equals(schema.getTag())) {
      currentTagTable = schema.getTag();
      registerSchema(schema);
    }

    long start = 0;
    try (Statement statement = connection.createStatement()) {
      for (Record record : records) {
        String[] sqls = getInsertOneBatchSql(schema, record.timestamp, record.fields);
        for (String sql : sqls) {
          statement.addBatch(sql);
          logger.debug(sql);
        }
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
  private String[] getInsertOneBatchSql(Schema schema, long timestamp, List<Object> values) {
    int splitNum = (int) Math.ceil(schema.getFields().length * 1.0 / fieldLimit);
    String[] sqls = new String[splitNum];
    for (int n = 1; n <= splitNum; n++) {
      StringBuilder builder_schema = new StringBuilder();
      StringBuilder builder_value = new StringBuilder();
      StringBuilder builder_all = new StringBuilder();
      builder_all.append("insert into ")
          .append(encapName(
              tagOneToMore(schema.getTag(), n))) // note table name add suffix start from 1
          .append("(time");
      for (int i = (n - 1) * fieldLimit; i < Math.min(schema.getFields().length, n * fieldLimit);
          i++) {
        builder_schema.append(",").append(encapName(schema.getFields()[i]));

        Object value = values.get(i);
        if (schema.getTypes()[i] == String.class && value != null) {
          // this is for replacing inner single quotes with double quotes, otherwise timescaledb go wrong
          // for example: '{'La':0.0,'Lo':0.0,'Satellite':0,'Speed':0,'Direction':0,'GSMSignal':255}'
          // the outer single quotes are removed while the inner is still there like 'La'
          value = ((String) value).replace('\'', '\"');

          builder_value.append(",'").append(value).append("'");
          // NOTE that outer quotes are removed during converting lines to records, so here need to be added for string type in TimescaleDB.
          // And if quotes are not removed during converting lines to records, 'abc' will be ''abc'' here, which is wrong format.
        } else {
          builder_value.append(",").append(value);
        }
      }
      builder_all.append(builder_schema.toString());
      builder_all.append(") values(");
      builder_all.append(timestamp);
      builder_all.append(builder_value.toString());
      builder_all.append(");");
      sqls[n - 1] = builder_all.toString();
    }
    return sqls;
  }
}

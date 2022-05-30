package cn.edu.thu.database.influxdb;

import cn.edu.thu.common.Config;
import cn.edu.thu.common.Record;
import cn.edu.thu.common.Schema;
import cn.edu.thu.database.IDataBaseManager;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import okhttp3.OkHttpClient;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.influxdb.dto.QueryResult.Result;
import org.influxdb.dto.QueryResult.Series;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InfluxDBManager implements IDataBaseManager {

  private InfluxDB influxDB;
  private static Logger logger = LoggerFactory.getLogger(InfluxDBManager.class);
  private String measurementId = "tb1";
  private String database;
  private Config config;

  private static String COUNT_SQL_WITH_TIME = "select count(%s) from %s where time >= %dms and time <= %dms and %s='%s'";

  private static String COUNT_SQL_WITHOUT_TIME = "select count(%s) from %s where %s ='%s'";


  public InfluxDBManager(Config config) {
    this.config = config;

    OkHttpClient.Builder okHttpClientBuilder = new OkHttpClient().newBuilder()
        .connectTimeout(5, TimeUnit.MINUTES)
        .readTimeout(5, TimeUnit.MINUTES)
        .writeTimeout(5, TimeUnit.MINUTES)
        .retryOnConnectionFailure(true);

    influxDB = InfluxDBFactory.connect(config.INFLUXDB_URL, okHttpClientBuilder);
    database = config.INFLUXDB_DATABASE;
  }

  @Override
  public void initServer() {
    influxDB.query(new Query("DROP DATABASE " + database, "_internal"));
    influxDB.query(new Query("CREATE DATABASE " + database, "_internal"));
//    close();
  }

  @Override
  public void initClient() {
  }

  @Override
  public long query() {
    // prepare sql
    String[] queryInfo = generateQuery();
    String queryDatabase = queryInfo[0];
    String sql = queryInfo[1];
    logger.info("Begin query: {}", sql);

    // begin execution
    final BlockingQueue<QueryResult> queue = new LinkedBlockingQueue<>();
    int cnt = 0;
    QueryResult result;
    long start = 0;
    long elapsedTime = 0;
    if (!config.QUERY_RESULT_PRINT_FOR_DEBUG) {
      start = System.nanoTime();
      // Use chunking query is to deal with the problem that large query results cannot fit within java heap space
      influxDB.query(new Query(sql, queryDatabase), config.INFLUXDB_QUERY_CHUNKING_SIZE,
          queue::add); // note influxdb chunking query is async.
      try {
        do {
          result = queue.poll(20, TimeUnit.SECONDS);
          if (result.getError() != null) {
            break;
          }
          for (Result res : result.getResults()) {
            List<Series> series = res.getSeries();
            if (series == null) {
              continue;
            }
            if (res.getError() != null) {
              logger.error(res.getError());
            }
            for (Series serie : series) {
              List<List<Object>> values = serie.getValues();
              cnt += values.size() * (serie.getColumns().size() - 1);
            }
          }
        } while (true);
        String end = result.getError();
        if (!end.equals(
            "DONE")) { // "Done" is the mark of query result end. Ref: https://github.com/influxdata/influxdb-java/pull/270
          logger.error("InfluxDB chunking query result went wrong: " + end);
        }
      } catch (Exception e) {
        logger.error("error: " + e);
      }
      elapsedTime = System.nanoTime() - start;
    } else {
      start = System.nanoTime();
      // Use chunking query is to deal with the problem that large query results cannot fit within java heap space
      influxDB.query(new Query(sql, queryDatabase), config.INFLUXDB_QUERY_CHUNKING_SIZE,
          queue::add); // note influxdb chunking query is async.
      try {
        do {
          result = queue.poll(20, TimeUnit.SECONDS);
          if (result.getError() != null) {
            break;
          }
          for (Result res : result.getResults()) {
            List<Series> series = res.getSeries();
            if (series == null) {
              continue;
            }
            if (res.getError() != null) {
              logger.error(res.getError());
            }
            for (Series serie : series) {
              List<List<Object>> values = serie.getValues();
              cnt += values.size() * (serie.getColumns().size() - 1);
            }
            logger.info(res.toString());
          }
        } while (true);
        String end = result.getError();
        if (!end.equals(
            "DONE")) { // "Done" is the mark of query result end. Ref: https://github.com/influxdata/influxdb-java/pull/270
          logger.error("InfluxDB chunking query result went wrong: " + end);
        }
      } catch (Exception e) {
        logger.error("error: " + e);
      }
      elapsedTime = System.nanoTime() - start;
    }
    logger.info("Query finished. Total points: {}. SQL: {}", cnt, sql);
    return elapsedTime;
  }

  /**
   * @return the first string denotes queryDatabase, the second string denotes sql
   */
  private String[] generateQuery() {
    String sql = null;
    String queryDatabase = null;
    switch (config.QUERY_TYPE) {
      case "SINGLE_SERIES_RAW_QUERY":
        // use yanchang dataset
        queryDatabase = "yanchang";
        sql = String.format(
            "select collecttime from tb1 where deviceId='root.T000100010002.90003' limit %d",
            config.QUERY_PARAM);
        break;
      case "MULTI_SERIES_ALIGN_QUERY":
        // use dianchang dataset
        queryDatabase = "dianchang";
        String sql_format = "select %s from tb1 where deviceId='root.DianChang.d1'";
        StringBuilder selectSensors = new StringBuilder();
        for (int i = 1; i < config.QUERY_PARAM + 1; i++) {
          selectSensors.append("sensor" + i);
          if (i < config.QUERY_PARAM) {
            selectSensors.append(",");
          }
        }
        sql = String.format(sql_format, selectSensors.toString());
        break;
      case "SINGLE_SERIES_COUNT_QUERY":
        // use yanchang dataset
        queryDatabase = "yanchang";
        switch (config.QUERY_PARAM) {
          case 1:
            sql = "select count(collecttime) from tb1 where deviceId='root.T000100010002.90003' and time<=1601023212859000000";
            break;
          case 100:
            sql = "select count(collecttime) from tb1 where deviceId='root.T000100010002.90003' and time<=1601023262692000000";
            break;
          case 10000:
            sql = "select count(collecttime) from tb1 where deviceId='root.T000100010002.90003' and time<=1601045811969000000";
            break;
          case 1000000:
            sql = "select count(collecttime) from tb1 where deviceId='root.T000100010002.90003' and time<=1604742917425000000";
            break;
          default:
            logger.error("QUERY_PARAM not correct! Please check your configurations.");
            break;
        }
        break;
      case "SINGLE_SERIES_DOWNSAMPLING_QUERY":
        // use yanchang dataset
        queryDatabase = "yanchang";
        sql = String.format(
            "select count(collecttime) from tb1 where deviceId='root.T000100010002.90003' "
                + "and time>=1601023212859000000 and time<=1602479033307000000 group by time(%dms)",
            config.QUERY_PARAM);
        break;
      default:
        logger.error("QUERY_TYPE not correct! Please check your configurations.");
        break;
    }
    return new String[]{queryDatabase, sql};
  }

  @Override
  public long flush() {
    return 0;
  }

  @Override
  public long close() {
    influxDB.close();
    return 0;
  }

  @Override
  public long insertBatch(List<Record> records, Schema schema) {

    // get data points
    List<Point> points = convertRecords(records, schema);
    BatchPoints batchPoints = BatchPoints.database(database).points(points.toArray(new Point[0]))
        .build();

    long start = System.nanoTime();
    try {
      influxDB.write(batchPoints);
    } catch (Exception e) {
      if (e.getMessage() != null && e.getMessage().contains("Failed to connect to")) {
        logger.error("InfluxDBManager is down!!!!!!");
      } else {
        e.printStackTrace();
      }
    }
    return System.nanoTime() - start;
  }

  private List<Point> convertRecords(List<Record> records, Schema schema) {
    List<Point> points = new ArrayList<>();
    for (Record record : records) {
      Point point = convertRecord(record, schema);
      if (point != null) {
        points.add(point);
      }
    }
    return points;
  }

  private Point convertRecord(Record record, Schema schema) {
    Map<String, String> tagSet = new HashMap<>();
    tagSet.put(Config.TAG_NAME, record.tag);

    HashMap<String, Object> fieldSet = new HashMap<>();
    for (int i = 0; i < schema.getFields().length; i++) {
      Object value = record.fields.get(i);
      if (value == null) {
        continue;
      }
      if (schema.getTypes()[i] == String.class) {
        // note that field had been removeOuterQuote in CSVReader
        fieldSet.put(schema.getFields()[i], "\"" + record.fields.get(i) + "\"");
        // 不过如果是string类型，本身这个record.fields.get(i)就是string类型吧
      } else {
        fieldSet.put(schema.getFields()[i], record.fields.get(i));
      }
    }

    if (fieldSet.isEmpty()) {
      return null; // TODO: solve bug: java.lang.IllegalArgumentException: Expecting a positive number for fields size
    } else {
      return Point.measurement(measurementId)
          .time(record.timestamp, TimeUnit.MILLISECONDS) //TODO: check this unit
          .tag(tagSet)
          .fields(fieldSet)
          .build();
    }
  }

}

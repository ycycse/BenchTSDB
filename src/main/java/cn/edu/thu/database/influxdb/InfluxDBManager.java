package cn.edu.thu.database.influxdb;

import cn.edu.thu.common.Config;
import cn.edu.thu.common.Record;
import cn.edu.thu.common.Schema;
import cn.edu.thu.database.IDataBaseManager;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
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
    influxDB = InfluxDBFactory.connect(config.INFLUXDB_URL);
    database = config.INFLUXDB_DATABASE;
  }

  @Override
  public void initServer() {
    influxDB.query(new Query("DROP DATABASE " + database));
    influxDB.query(new Query("CREATE DATABASE " + database));
//    close();
  }

  @Override
  public void initClient() {
  }

  @Override
  public long query() {

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
            sql = "select count(collecttime) from tb1 where deviceId='root.T000100010002.90003' and time<=1602131946370000000";
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
            "select first(collecttime) from tb1 where deviceId='root.T000100010002.90003' "
                + "and time>=1601023212859000000 and time<=1602479033307000000 group by time(%dms)",
            config.QUERY_PARAM);
        break;
      default:
        logger.error("QUERY_TYPE not correct! Please check your configurations.");
        break;
    }
    logger.info("Begin query: {}", sql);

    long start = System.nanoTime();
    QueryResult queryResult = influxDB.query(new Query(sql, queryDatabase));
    long elapsedTime = System.nanoTime() - start;

    logger.info("Query {} finished. Total lines: {}", sql,
        queryResult.getResults().get(0).getSeries().get(0).getValues().size());
    return elapsedTime;
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
      points.add(point);
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
      fieldSet.put(schema.getFields()[i], record.fields.get(i));
    }

    return Point.measurement(measurementId)
        .time(record.timestamp, TimeUnit.MILLISECONDS) //TODO: check this unit
        .tag(tagSet)
        .fields(fieldSet)
        .build();
  }

}

package cn.edu.thu.database.kairosdb;

import cn.edu.thu.common.Config;
import cn.edu.thu.common.Record;
import cn.edu.thu.common.Schema;
import cn.edu.thu.common.ThuHttpRequest;
import cn.edu.thu.database.IDataBaseManager;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KairosDBManager implements IDataBaseManager {

  private static final Logger logger = LoggerFactory.getLogger(KairosDBManager.class);
  private Config config;
  private String queryUrl;
  private String writeUrl;
  private String deleteUrl;
  private String url;

  private static final String QUERY_START_TIME = "start_absolute";
  private static final String QUERY_END_TIME = "end_absolute";

  public KairosDBManager(Config config) {
    this.config = config;
    this.url = config.KAIROSDB_URL;
    queryUrl = url + "/api/v1/datapoints/query";
    writeUrl = url + "/api/v1/datapoints";
    deleteUrl = url + "/api/v1/metric/%s";
    if (config.KAIROSDB_BATCH_POINTS) {
      logger.info(
          "use batched data points API. See https://kairosdb.github.io/docs/restapi/AddDataPoints.html.");
    } else {
      logger.info(
          "use single data point API. See https://kairosdb.github.io/docs/restapi/AddDataPoints.html.");
    }
  }

  @Override
  public void initServer() {
//    for (String sensor : config.FIELDS) {
//      try {
//        ThuHttpRequest.sendDelete(String.format(deleteUrl, sensor), "");
//      } catch (IOException e) {
//        logger.error("Delete metric {} failed when initializing KairosDBManager.", sensor);
//        e.printStackTrace();
//      }
//    }
    logger.info(
        "NOTE: I don't clear existent data for KairosDB. Please you assure KairosDB is started up with correct Cassandra.");
    logger.info("ATTENTION: if you drop keyspace kairosdb in Cassandra, all data will be dropped.");
//    logger.info("To reset your KairosDB database to default, run something like that: "
//        + "(1) Enter Cassandra Query Language terminal by running cqlsh. "
//        + "(2) Drop kairosdb keyspace: cqlsh> DROP KEYSPACE kairosdb ; "
//        + "(3) Delete the kairosdb data directory in Cassandra."
//        + "(4) Running a fresh new Kairosdb server. ");
  }

  @Override
  public void initClient() {

  }

  @Override
  public long insertBatch(List<Record> records, Schema schema) {
    List<KairosDBPoint> points;

    logger.info("Begin converting records to KairosDBPoints...");
    // convert to kairosdb data points
    if (!config.KAIROSDB_BATCH_POINTS) {
      /*
       use “timestamp” with “value” for a single data point。
       be like:
       {"name":"struct@waste%105008","tags":{"deviceId":"root.T000100010002.90003"},"timestamp":1601168451210,"value":0.0},
       {"name":"struct@waste%105008","tags":{"$ref":"$[26815].tags"},"timestamp":1601168452216,"value":0.0},
       ...
       */
      points = new ArrayList<>();
      for (Record record : records) {
        points.addAll(convertToSinglePoints(record, schema));
      }
    } else {
      /*
       use “datapoints” to post multiple data points。
       be like:
       {
        "name":"struct@waste%105008",
        "tags":{"deviceId":"root.T000100010002.90003"},
        "datapoints":[[1601168451210,0.0],[1601168452216,0.0],...]
        }
       */
      points = convertToBatchedPoints(records, schema);
    }
    logger.info("Finish converting records to KairosDBPoints.");

    String body = JSON.toJSONString(points, SerializerFeature.DisableCircularReferenceDetect);

    long start = System.nanoTime();
//    String response = null;
    try {
      ThuHttpRequest.sendPost(writeUrl, body);
//      logger.info("response: {}", response); // comment this to avoid logger.info time cost
    } catch (IOException e) {
      e.printStackTrace();
      logger.error("meet error when writing: {}", e.getMessage());
    }

    return System.nanoTime() - start;
  }


  private List<KairosDBPoint> convertToBatchedPoints(List<Record> records, Schema schema) {
    // TODO: 这里默认要求一个batch的records的tag、sensors都是一样的

    List<KairosDBPoint> points = new ArrayList<>();
    Map<String, String> tags = new HashMap<>();
    tags.put(Config.TAG_NAME, schema.getTag());
    for (String sensor : schema.getFields()) {
      KairosDBPoint point = new KairosDBPoint();
      point.setTags(tags);
      point.setName(sensor);
      points.add(point);
    }

    for (Record record : records) {
      for (int i = 0; i < schema.getFields().length; i++) {
        Object value = record.fields.get(i);
        if (value == null) {
          continue;
        }
        KairosDBPoint kairosDBPoint = points.get(i);
        List<Object> point = new ArrayList<>();
        point.add(record.timestamp);
        if (schema.getTypes()[i] == String.class) {
          point.add("\"" + record.fields.get(i)
              + "\""); // note that field had been removeOuterQuote in CSVReader
          // 不过如果是string类型，本身这个record.fields.get(i)就是string类型吧。直接add(value)可能就不行。
        } else {
          point.add(record.fields.get(i));
        }

        kairosDBPoint.addDatapoints(point);
      }
    }

    // remove KairosDBPoint that has empty datapoint list
    points.removeIf(point -> point.getDatapoints() == null);

    return points;
  }

  private List<KairosDBPoint> convertToSinglePoints(Record record, Schema schema) {
    List<KairosDBPoint> points = new ArrayList<>();

    Map<String, String> tags = new HashMap<>();
    tags.put(Config.TAG_NAME, record.tag);

    for (int i = 0; i < schema.getFields().length; i++) {
      Object value = record.fields.get(i);
      if (value == null) {
        continue;
      }
      KairosDBPoint point = new KairosDBPoint();
      point.setName(schema.getFields()[i]);
      point.setTimestamp(record.timestamp);
      if (schema.getTypes()[i] == String.class) {
        point.setValue(
            "\"" + record.fields.get(i)
                + "\""); // note that field had been removeOuterQuote in CSVReader
      } else {
        point.setValue(record.fields.get(i));
      }
      point.setTags(tags);
      points.add(point);
    }
    return points;
  }


  /**
   * { "start_absolute": 1357023600000, "end_relative": { "value": "5", "unit": "days" },
   * "time_zone": "Asia/Kabul", "metrics": [ { "tags": { "host": ["foo", "foo2"], "customer":
   * ["bar"] }, "name": "abc.123", "limit": 10000, "aggregators": [ { "name": "sum", "sampling": {
   * "value": 10, "unit": "minutes" } } ] }, { "tags": { "host": ["foo", "foo2"], "customer":
   * ["bar"] }, "name": "xyz.123", "aggregators": [ { "name": "avg", "sampling": { "value": 10,
   * "unit": "minutes" } } ] } ] }
   */
  @Override
  public long query() {

    String json = generateQuery();
    logger.info("Begin query：{}", json);

    int resStrLen = 0;
    long start = 0;
    long elapsedTime = 0;
    if (!config.QUERY_RESULT_PRINT_FOR_DEBUG) {
      start = System.nanoTime();
      try {
        String res = ThuHttpRequest.sendPost(queryUrl, json);
        resStrLen = res.length();
      } catch (IOException e) {
        e.printStackTrace();
      }
      elapsedTime = System.nanoTime() - start;
    } else {
      start = System.nanoTime();
      try {
        String res = ThuHttpRequest.sendPost(queryUrl, json);
        resStrLen = res.length();
        logger.info(res);
      } catch (IOException e) {
        e.printStackTrace();
      }
      elapsedTime = System.nanoTime() - start;
    }
    logger.info("Query finished. Response json string length: {}. SQL: {}", resStrLen, json);
    return elapsedTime;
  }

  private String generateQuery() {
    Map<String, Object> queryMap = new HashMap<>();
    switch (config.QUERY_TYPE) {
      case "SINGLE_SERIES_RAW_QUERY":
        // select collecttime from root.T000100010002.90003 limit %d
        queryMap.put(QUERY_START_TIME, 1601023212859L);

        List<Map<String, Object>> subQueries = new ArrayList<>();
        Map<String, Object> subQuery = new HashMap<>();
        subQuery.put("name", "collecttime");
        subQuery.put("limit", config.QUERY_PARAM);
        Map<String, List<String>> tags = new HashMap<>();
        List<String> tagVs = new ArrayList<>();
        tagVs.add("root.T000100010002.90003");
        tags.put("deviceId", tagVs);
        subQuery.put("tags", tags);

        subQueries.add(subQuery);
        queryMap.put("metrics", subQueries);
        break;
      case "MULTI_SERIES_ALIGN_QUERY":
        // select %s from root.DianChang.d1
        queryMap.put(QUERY_START_TIME, 1577836800000L);

        subQueries = new ArrayList<>();
        for (int i = 1; i < config.QUERY_PARAM + 1; i++) {
          subQuery = new HashMap<>();
          subQuery.put("name", "sensor" + i);
          tags = new HashMap<>();
          tagVs = new ArrayList<>();
          tagVs.add("root.DianChang.d1");
          tags.put("deviceId", tagVs);
          subQuery.put("tags", tags);

          subQueries.add(subQuery);
        }
        queryMap.put("metrics", subQueries);
        break;
      case "SINGLE_SERIES_COUNT_QUERY":
        // select count(collecttime) from root.T000100010002.90003 where time<=1601023212859
        queryMap.put(QUERY_START_TIME, 1601023212859L);
        switch (config.QUERY_PARAM) {
          case 1:
            queryMap.put(QUERY_END_TIME, 1601023212859L + 1);
            break;
          case 100:
            queryMap.put(QUERY_END_TIME, 1601023262692L + 1);
            break;
          case 10000:
            queryMap.put(QUERY_END_TIME, 1601045811969L + 1);
            break;
          case 1000000:
            queryMap.put(QUERY_END_TIME, 1602131946370L + 1);
            break;
          default:
            logger.error("QUERY_PARAM not correct! Please check your configurations.");
            break;
        }

        subQueries = new ArrayList<>();
        subQuery = new HashMap<>();
        subQuery.put("name", "collecttime");
        tags = new HashMap<>();
        tagVs = new ArrayList<>();
        tagVs.add("root.T000100010002.90003");
        tags.put("deviceId", tagVs);
        subQuery.put("tags", tags);

        List<Map<String, Object>> aggregators = new ArrayList<>();
        Map<String, Object> aggregator = new HashMap<>();
        aggregator.put("name", "count");
        Map<String, Object> sampling = new HashMap<>();
        sampling.put("value", 100);
        sampling.put("unit", "years");
        // “milliseconds”, “seconds”, “minutes”, “hours”, “days”, “weeks”, “months”, and “years”
        aggregator.put("sampling", sampling);
        aggregators.add(aggregator);
        subQuery.put("aggregators", aggregators);

        subQueries.add(subQuery);
        queryMap.put("metrics", subQueries);
        break;
      case "SINGLE_SERIES_DOWNSAMPLING_QUERY":
        // select count(collecttime) from root.T000100010002.90003 group by ([1601023212859, 1602479033308), 1ms)

        subQueries = new ArrayList<>();
        subQuery = new HashMap<>();
        subQuery.put("name", "collecttime");
        tags = new HashMap<>();
        tagVs = new ArrayList<>();
        tagVs.add("root.T000100010002.90003");
        tags.put("deviceId", tagVs);
        subQuery.put("tags", tags);

        aggregators = new ArrayList<>();
        aggregator = new HashMap<>();
        aggregator.put("name", "count");
        aggregator.put("align_start_time", true);
        sampling = new HashMap<>();
        sampling.put("value", config.QUERY_PARAM);
        sampling.put("unit", "milliseconds");
        // “milliseconds”, “seconds”, “minutes”, “hours”, “days”, “weeks”, “months”, and “years”
        aggregator.put("sampling", sampling);
        aggregators.add(aggregator);
        subQuery.put("aggregators", aggregators);

        switch (config.QUERY_PARAM) { // note that the startTime is modified to align with influxdb group by time style
          case 1:
            queryMap.put(QUERY_START_TIME, 1601023212859L);
            break;
          case 100:
            queryMap.put(QUERY_START_TIME, 1601023212800L);
            break;
          case 10000:
            queryMap.put(QUERY_START_TIME, 1601023210000L);
            break;
          case 1000000:
            queryMap.put(QUERY_START_TIME, 1601023000000L);
            break;
          default:
            logger.error("QUERY_PARAM not correct! Please check your configurations.");
            break;
        }
        subQueries.add(subQuery);
        queryMap.put("metrics", subQueries);
        break;
      default:
        logger.error("QUERY_TYPE not correct! Please check your configurations.");
        break;
    }

    return JSON.toJSONString(queryMap);
  }

  @Override
  public long flush() {
    return 0;
  }

  @Override
  public long close() {
    return 0;
  }
}

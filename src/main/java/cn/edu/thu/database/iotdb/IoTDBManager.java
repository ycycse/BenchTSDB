package cn.edu.thu.database.iotdb;

import cn.edu.thu.common.Config;
import cn.edu.thu.common.Record;
import cn.edu.thu.common.Schema;
import cn.edu.thu.database.IDataBaseManager;
import cn.edu.thu.database.fileformat.TsFileManager;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.encoding.encoder.Encoder;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.BitMap;
import org.apache.iotdb.tsfile.write.record.NonAlignedTablet;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IoTDBManager implements IDataBaseManager {

  private static final Logger logger = LoggerFactory.getLogger(
      IoTDBManager.class);
  private Config config;
  private Session session;

  public IoTDBManager(Config config) {
    this.config = config;
    session =
        new Session(
            config.IOTDB_HOST,
            config.IOTDB_PORT,
            config.IOTDB_USERNAME,
            config.IOTDB_PASSWORD,
            true);
  }

  @Override
  public void initServer() {
    try {
      if (config.IOTDB_ENABLE_THRIFT_COMPRESSION) {
        session.open(true);
      } else {
        session.open();
      }
      session.deleteStorageGroup(config.IOTDB_STORAGE_GROUP);
      session.setStorageGroup(config.IOTDB_STORAGE_GROUP);
      session.close();
    } catch (StatementExecutionException | IoTDBConnectionException e) {
      logger.error("Failed to add session", e);
    }
  }

  @Override
  public void initClient() {
    try {
      if (config.IOTDB_ENABLE_THRIFT_COMPRESSION) {
        session.open(true);
      } else {
        session.open();
      }
    } catch (IoTDBConnectionException e) {
      logger.error("Failed to add session", e);
    }
  }

  @Override
  public long insertBatch(List<Record> records, Schema schema) { // use insertTablet interface
//    Tablet tablet = genTablet(records, schema); // Note this part of time is not included
    long start = System.nanoTime();
    try {
      if (config.useAlignedTablet) {
        insertBatchAligned(records, session, schema);
      } else {
        insertBatchNonAligned(records, session, schema);
      }
    } catch (IOException e) {
      logger.error(e.getMessage());
    }
    return System.nanoTime() - start;
  }

  private void insertBatchAligned(List<Record> records, Session session, Schema schema) {
    Tablet tablet = convertToTablet(records, schema);
    try {
//      if (config.useAlignedSeries) { //TODO: whether to use this?
//        writer.writeAligned(tablet);
//      } else {
//      session.insertAlignedTablet(tablet);
      /** TODO
       * -   **session.insertTablet(tablet) + mark denotes null: right**
       * -   **session.insertAlignedTablet(tablet) + mark denotes null: lose data**
       * -   **session.insertAlignedTablet(tablet) + mark denotes existent: total wrong**
       */
      session.insertTablet(tablet);
//      }
    } catch (Exception e) {
      logger.error("Insert {} records failed, schema {}, ", records.size(), schema, e);
    }
  }

  private Tablet convertToTablet(List<Record> records, Schema schema) {
    List<MeasurementSchema> schemaList = new ArrayList<>();
    for (int i = 0; i < schema.getFields().length; i++) {
      Map<String, String> props = new HashMap<>();
      props.put(Encoder.MAX_POINT_NUMBER, schema.getPrecision()[i] + "");
      MeasurementSchema measurementSchema = new MeasurementSchema(schema.getFields()[i],
          toTsDataType(schema.getTypes()[i]),
          toTsEncoding(schema.getTypes()[i]), CompressionType.SNAPPY, props);
      schemaList.add(measurementSchema);
    }

    Tablet tablet = new Tablet(schema.getTag(), schemaList, records.size());

    long[] timestamps = tablet.timestamps;
    Object[] values = tablet.values;
    tablet.bitMaps = new BitMap[values.length];
    for (int i = 0; i < tablet.bitMaps.length; i++) {
      tablet.bitMaps[i] = new BitMap(records.size());
    }

    for (Record record : records) {
      int row = tablet.rowSize++;
      timestamps[row] = record.timestamp;
      for (int i = 0; i < schema.getFields().length; i++) {
        addToColumn(tablet.values[i], row, record.fields.get(i), tablet.bitMaps[i],
            schema.getTypes()[i]);
      }
    }

    return tablet;
  }

  private void addToColumn(Object column, int rowIndex, Object field, BitMap bitMap,
      Class<?> type) {
    if (type == Long.class) {
      addToLongColumn(column, rowIndex, field, bitMap);
    } else if (type == Double.class) {
      addToDoubleColumn(column, rowIndex, field, bitMap);
    } else {
      addToTextColumn(column, rowIndex, field, bitMap);
    }
  }

  private void addToDoubleColumn(Object column, int rowIndex, Object field, BitMap bitMap) {
    double[] sensor = (double[]) column;
    sensor[rowIndex] = field != null ? (double) field : Double.MIN_VALUE;
    if (field == null) {
      bitMap.mark(rowIndex);
    }
  }

  private void addToLongColumn(Object column, int rowIndex, Object field, BitMap bitMap) {
    long[] sensor = (long[]) column;
    sensor[rowIndex] = field != null ? (long) field : Long.MIN_VALUE;
    if (field == null) {
      bitMap.mark(rowIndex);
    }
  }

  private void addToTextColumn(Object column, int rowIndex, Object field, BitMap bitMap) {
    Binary[] sensor = (Binary[]) column;
    sensor[rowIndex] = field != null ? new Binary((String) field) : Binary.EMPTY_VALUE;
    if (field == null) {
      bitMap.mark(rowIndex);
    }
  }


  private void insertBatchNonAligned(List<Record> records, Session session, Schema schema)
      throws IOException {
    throw new IOException("not implemented");
//    NonAlignedTablet tablet = convertToNonAlignedTablet(records, schema);
//    try {
//      session.insertTablet(tablet);
//    } catch (Exception e) {
//      logger.error("Insert {} records failed, schema {}, ", records.size(), schema, e);
//    }
  }

  private NonAlignedTablet convertToNonAlignedTablet(List<Record> records,
      Schema schema) {
    return null;
  }

//  private static void addToColumn(Object column, int rowIndex, Object field, BitMap bitMap,
//      Class<?> type) {
//    if (type == Long.class) {
//      addToLongColumn(column, rowIndex, field, bitMap);
//    } else if (type == Double.class) {
//      addToDoubleColumn(column, rowIndex, field, bitMap);
//    } else {
//      addToTextColumn(column, rowIndex, field, bitMap);
//    }
//  }
//
//  private static void addToDoubleColumn(Object column, int rowIndex, Object field, BitMap bitMap) {
//    double[] sensor = (double[]) column;
//    sensor[rowIndex] = field != null ? (double) field : Double.MIN_VALUE;
//    if (field == null) {
//      bitMap.mark(rowIndex);
//    }
//  }
//
//  private static void addToLongColumn(Object column, int rowIndex, Object field, BitMap bitMap) {
//    long[] sensor = (long[]) column;
//    sensor[rowIndex] = field != null ? (long) field : Long.MIN_VALUE;
//    if (field == null) {
//      bitMap.mark(rowIndex);
//    }
//  }
//
//  private static void addToTextColumn(Object column, int rowIndex, Object field, BitMap bitMap) {
//    Binary[] sensor = (Binary[]) column;
//    sensor[rowIndex] = field != null ? new Binary((String) field) : Binary.EMPTY_VALUE;
//    if (field == null) {
//      bitMap.mark(rowIndex);
//    }
//  }

  private static TSDataType toTsDataType(Class<?> type) {
    if (type == Long.class) {
      return TSDataType.INT64;
    } else if (type == Double.class) {
      return TSDataType.DOUBLE;
    } else {
      return TSDataType.TEXT;
    }
  }

  private static TSEncoding toTsEncoding(Class<?> type) {
    if (type == Long.class) {
      return TSEncoding.RLE;
    } else if (type == Double.class) {
      return TSEncoding.GORILLA;
    } else {
      return TSEncoding.PLAIN;
    }
  }

  @Override
  public long count(String tagValue, String field, long startTime, long endTime) {
//    Map<String, Object> queryMap = new HashMap<>();
//    queryMap.put("msResolution", true);
//
//    Map<String, Object> subQuery = new HashMap<>();
//
//    // query tag
//    Map<String, String> subsubQuery = new HashMap<>();
//    subsubQuery.put(Config.TAG_NAME, tagValue);
//    subQuery.put("tags", subsubQuery);
//
//    if (startTime == -1 || endTime == -1) {
//      logger.error("do not support");
//      return -1;
//    } else {
//      long diff = endTime - startTime;
//      queryMap.put("start", startTime);
//      subQuery.put("downsample", (diff) + "ms-count");
//
//    }
//
//    subQuery.put("metric", field);
//    subQuery.put("aggregator", "none");
//
//    List<Map<String, Object>> queries = new ArrayList<>();
//    queries.add(subQuery);
//    queryMap.put("queries", queries);
//
//    String sql = JSON.toJSONString(queryMap);
//
//    logger.info("sql: {}", sql);
    long start = System.nanoTime();
//
//    try {
//      String response = ThuHttpRequest.sendPost(queryUrl, sql);
//      logger.info(response);
//    } catch (IOException e) {
//      e.printStackTrace();
//    }
//
    return System.nanoTime() - start;
  }

  @Override
  public long flush() {
    return 0;
  }

  @Override
  public long close() {
    try {
      session.close();
    } catch (IoTDBConnectionException ioTDBConnectionException) {
      logger.error("Failed to close session.");
    }
    return 0;
  }

}

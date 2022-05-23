package cn.edu.thu.database.iotdb;

import cn.edu.thu.common.Config;
import cn.edu.thu.common.Record;
import cn.edu.thu.common.Schema;
import cn.edu.thu.database.IDataBaseManager;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionDataSet;
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
    long elapsedTime = 0;
    try {
      if (config.useAlignedTablet) {
        elapsedTime = insertBatchAligned(records, session, schema);
      } else {
        elapsedTime = insertBatchNonAligned(records, session, schema);
      }
    } catch (IOException e) {
      logger.error(e.getMessage());
    }
    return elapsedTime;
  }

  private long insertBatchAligned(List<Record> records, Session session, Schema schema) {
    Tablet tablet = convertToTablet(records, schema);
    long start = System.nanoTime();
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
    return System.nanoTime() - start;
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


  private long insertBatchNonAligned(List<Record> records, Session session, Schema schema)
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
  public long query() {
    String sql = null;
    switch (config.QUERY_TYPE) {
      case "SINGLE_SERIES_RAW_QUERY":
        // use yanchang dataset
        sql = String.format("select collecttime from root.T000100010002.90003 limit %d",
            config.QUERY_PARAM);
        break;
      case "MULTI_SERIES_ALIGN_QUERY":
        // use dianchang dataset
        String sql_format = "select %s from root.DianChang.d1";
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
        switch (config.QUERY_PARAM) {
          case 1:
            sql = "select count(collecttime) from root.T000100010002.90003 where time<=1601023212859";
            break;
          case 100:
            sql = "select count(collecttime) from root.T000100010002.90003 where time<=1601023262692";
            break;
          case 10000:
            sql = "select count(collecttime) from root.T000100010002.90003 where time<=1601045811969";
            break;
          case 1000000:
            sql = "select count(collecttime) from root.T000100010002.90003 where time<=1602131946370";
            break;
          default:
            logger.error("QUERY_PARAM not correct! Please check your configurations.");
            break;
        }
        break;
      case "SINGLE_SERIES_DOWNSAMPLING_QUERY":
        // use yanchang dataset
        switch (config.QUERY_PARAM) { // note that the startTime is modified to align with influxdb group by time style
          case 1:
            sql = "select first_value(collecttime) from root.T000100010002.90003 group by ([1601023212859, 1602479033308), 1ms)";
            break;
          case 100:
            sql = "select first_value(collecttime) from root.T000100010002.90003 group by ([1601023212800, 1602479033308), 100ms)";
            break;
          case 10000:
            sql = "select first_value(collecttime) from root.T000100010002.90003 group by ([1601023210000, 1602479033308), 10000ms)";
            break;
          case 1000000:
            sql = "select first_value(collecttime) from root.T000100010002.90003 group by ([1601023000000, 1602479033308), 1000000ms)";
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
    logger.info("Begin query: {}", sql);

    int c = 0;
    long start = System.nanoTime();
    try (SessionDataSet dataSet = session.executeQueryStatement(sql)) {
      while (dataSet.hasNext()) {
        dataSet.next();
        c++;
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    long elapsedTime = System.nanoTime() - start;

    logger.info("Query {} finished. Total lines: {}", sql, c);
    return elapsedTime;
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

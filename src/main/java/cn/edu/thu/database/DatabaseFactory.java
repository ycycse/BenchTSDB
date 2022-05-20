package cn.edu.thu.database;

import cn.edu.thu.common.Config;
import cn.edu.thu.database.fileformat.ORCManager;
import cn.edu.thu.database.fileformat.ParquetManager;
import cn.edu.thu.database.fileformat.TsFileManager;
import cn.edu.thu.database.influxdb.InfluxDBManager;
import cn.edu.thu.database.iotdb.IoTDBManager;
import cn.edu.thu.database.kairosdb.KairosDBManager;
import cn.edu.thu.database.opentsdb.OpenTSDBManager;
import cn.edu.thu.database.timescaledb.TimescaleDBManager;
//import cn.edu.thu.database.waterwheel.WaterWheelManager;

public class DatabaseFactory {

  public static IDataBaseManager getDbManager(Config config) {
    switch (config.DATABASE) {
      case "NULL":
        return new NullManager();
      case "IOTDB": // v0.14.0-snapshot
        return new IoTDBManager(config);
      case "INFLUXDB": // v1.8.10
        return new InfluxDBManager(config);
      case "KAIROSDB": // v1.3.0 on Cassandra v3.11.13
        return new KairosDBManager(config);
      case "TIMESCALEDB": // v2.6.1 on PostgreSQL v14.3
        return new TimescaleDBManager(config);
//      case "OPENTSDB":
//        return new OpenTSDBManager(config);
//      case "SUMMARYSTORE":
//        return new SummaryStoreManager(config);
//      case "WATERWHEEL":
//        return new WaterWheelManager(config);
      case "TSFILE":
        return new TsFileManager(config);
      case "PARQUET":
        return new ParquetManager(config);
      case "ORC":
        return new ORCManager(config);
      default:
        throw new RuntimeException(config.DATABASE + " not supported");
    }
  }

}

package cn.edu.thu.writer;

import backup.MLabUtilizationReader;
import cn.edu.thu.common.Config;
import cn.edu.thu.common.Record;
import cn.edu.thu.common.Statistics;
import cn.edu.thu.database.DatabaseFactory;
import cn.edu.thu.database.IDataBaseManager;
import cn.edu.thu.reader.BasicReader;
import cn.edu.thu.reader.CSVReader;
import cn.edu.thu.reader.GeolifeReader;
import cn.edu.thu.reader.NOAAReader;
import cn.edu.thu.reader.ReddReader;
import cn.edu.thu.reader.SyntheticReader;
import cn.edu.thu.reader.TDriveReader;
import java.io.IOException;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RealDatasetWriter implements Runnable {

  private static Logger logger = LoggerFactory.getLogger(RealDatasetWriter.class);
  private IDataBaseManager database;
  private Config config;
  private BasicReader reader;
  private final Statistics statistics;

  public RealDatasetWriter(Config config, List<String> files, final Statistics statistics)
      throws IOException {
    this.database = DatabaseFactory.getDbManager(config);
    database.initClient();
    this.config = config;
    this.statistics = statistics;

    logger.info("thread construct!, need to read {} files", files.size());

    switch (config.DATA_SET) {
      case "NOAA":
        reader = new NOAAReader(config, files);
        break;
      case "GEOLIFE":
        reader = new GeolifeReader(config, files);
        break;
      case "TDRIVE":
        reader = new TDriveReader(config, files);
        break;
      case "MLAB_UTILIZATION":
        reader = new MLabUtilizationReader(config, files);
        break;
      case "REDD":
        reader = new ReddReader(config, files);
        break;
      case "SYNTHETIC":
        reader = new SyntheticReader(config);
        break;
      case "CSV":
        reader = new CSVReader(config, files);
        break;
      default:
        throw new RuntimeException(config.DATA_SET + " not supported");
    }

  }

  @Override
  public void run() {

    try {

      while (reader.hasNext()) {
        List<Record> batch = reader.next();
        long elapsedTime = database.insertBatch(batch, reader.getCurrentSchema());
        statistics.addLatency(elapsedTime);
        statistics.timeCost.addAndGet(elapsedTime);
        statistics.recordNum.addAndGet(batch.size());
        statistics.pointNum.addAndGet(batch.size() * reader.getCurrentSchema().getFields().length);
        logger.info("batch size: " + batch.size());
        logger.info(
            "Exp:{} ING. Current records:{}, points:{}, time:{} ms, speed:{} pts/s, "
                + "average latency:{} ms, "
                + "latency circular list(length:{},unit:ns):{}",
            config.EXP_NAME, statistics.recordNum, statistics.pointNum,
            (float) statistics.timeCost.get() / 1000_000F, statistics.speed(),
            statistics.getAverageLatencyInMillisecond(),
            statistics.writeLatency.size(),
            statistics.writeLatency
        );
      }

      statistics.timeCost.addAndGet(database.flush());
      statistics.timeCost.addAndGet(database.close());
    } catch (Exception e) {
      logger.warn("Exception during write", e);
    }

  }

}

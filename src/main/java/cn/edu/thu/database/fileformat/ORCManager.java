package cn.edu.thu.database.fileformat;

import cn.edu.thu.common.Config;
import cn.edu.thu.common.Record;
import cn.edu.thu.database.IDataBaseManager;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.orc.*;
import org.apache.orc.storage.ql.exec.vector.BytesColumnVector;
import org.apache.orc.storage.ql.exec.vector.DoubleColumnVector;
import org.apache.orc.storage.ql.exec.vector.LongColumnVector;
import org.apache.orc.storage.ql.exec.vector.VectorizedRowBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * time, seriesid, value
 *
 * time, deviceId, s1, s2, s3...
 *
 * time, series1, series2...
 */
public class ORCManager implements IDataBaseManager {

  private static Logger logger = LoggerFactory.getLogger(ORCManager.class);
  private Writer[] writers;
  private TypeDescription schema;
  private Config config;
  private String filePath;

  public ORCManager(Config config) {
    this.config = config;
    this.filePath = config.FILE_PATH;
  }

  public ORCManager(Config config, int threadNum) {
    this.config = config;
    this.filePath = config.FILE_PATH + "_" + threadNum;
  }

  @Override
  public void initServer() {

  }

  @Override
  public void initClient() {
    if (Config.FOR_QUERY) {
      return;
    }

    schema = TypeDescription.fromString(genWriteSchema());
    createWriters();
  }

  private void createWriters() {
    int fileNum = 1;
    if (config.useSynthetic && config.splitFileByDevice) {
      fileNum = config.syntheticDeviceNum;
    }
    writers = new Writer[fileNum];

    for (int i = 0; i < fileNum; i++) {
      String fullFilePath = i + "_" + filePath;
      new File(fullFilePath).delete();
      try {
        writers[i] = OrcFile.createWriter(new Path(fullFilePath),
            OrcFile.writerOptions(new Configuration())
                .setSchema(schema)
                .compress(CompressionKind.SNAPPY)
                .version(OrcFile.Version.V_0_12));
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  @Override
  public long insertBatch(List<Record> records) {

    long start = System.nanoTime();

    VectorizedRowBatch batch = schema.createRowBatch(records.size());

    for (int i = 0; i < records.size(); i++) {
      Record record = records.get(i);
      LongColumnVector time = (LongColumnVector) batch.cols[0];
      time.vector[i] = record.timestamp;

      if (!config.splitFileByDevice) {
        BytesColumnVector device = (BytesColumnVector) batch.cols[1];
        device.setVal(i, record.tag.getBytes(StandardCharsets.UTF_8));
      }

      for (int j = 0; j < config.FIELDS.length; j++) {
        DoubleColumnVector v;
        if (!config.splitFileByDevice) {
          v = (DoubleColumnVector) batch.cols[j + 2];
        } else {
          v = (DoubleColumnVector) batch.cols[j + 1];
        }
        v.vector[i] = (double) record.fields.get(j);
      }

      batch.size++;

      // If the batch is full, write it out and start over. actually not needed here
      if (batch.size == batch.getMaxSize()) {
        try {
          getWriter(record.tag).addRowBatch(batch);
        } catch (IOException e) {
          e.printStackTrace();
        }
        batch.reset();
      }
    }

    return System.nanoTime() - start;
  }

  private Writer getWriter(String tag) {
    if (config.splitFileByDevice) {
      return writers[getFileIndex(tag)];
    } else {
      return writers[0];
    }
  }

  private int getFileIndex(String tag) {
    // root.device_i
    return Integer.parseInt(tag.split("_")[1]);
  }

  private String genWriteSchema() {
    String s;
    if (config.splitFileByDevice) {
      s = "struct<timestamp:bigint";
    } else {
      s = "struct<timestamp:bigint,deviceId:string";
    }

    for (int i = 0; i < config.FIELDS.length; i++) {
      s += ("," + config.FIELDS[i] + ":" + "DOUBLE");
    }
    s += ">";
    return s;
  }

  private String getReadSchema(String field) {
    if (!config.splitFileByDevice) {
      return "struct<timestamp:bigint,deviceId:string," + field + ":DOUBLE>";
    } else {
      return "struct<timestamp:bigint," + field + ":DOUBLE>";
    }
  }

  @Override
  public long count(String tagValue, String field, long startTime, long endTime) {

    long start = System.nanoTime();

    String schema = getReadSchema(field);
    try {
      Reader reader = OrcFile.createReader(new Path(getFileIndex(tagValue) + "_" + filePath),
          OrcFile.readerOptions(new Configuration()));
      TypeDescription readSchema = TypeDescription.fromString(schema);

      VectorizedRowBatch batch = readSchema.createRowBatch();
      RecordReader rowIterator = reader.rows(reader.options().schema(readSchema));

      int fieldId;

      for (fieldId = 0; fieldId < config.FIELDS.length; fieldId++) {
        if (field.endsWith(config.FIELDS[fieldId])) {
          break;
        }
      }

      int result = 0;
      while (rowIterator.nextBatch(batch)) {
        for (int r = 0; r < batch.size; ++r) {

          // time, deviceId, field
          long t = ((LongColumnVector) batch.cols[0]).vector[r];
          if (t < startTime || t > endTime) {
            continue;
          }

          if (!config.splitFileByDevice) {
            String deviceId = ((BytesColumnVector) batch.cols[1]).toString(r);
            if (deviceId.endsWith(tagValue)) {
              result++;
            }
            double fieldValue = ((DoubleColumnVector) batch.cols[2]).vector[r];
          } else {
            result++;
            double fieldValue = ((DoubleColumnVector) batch.cols[1]).vector[r];
          }
        }
      }
      rowIterator.close();

      logger.info("ORC result: {}", result);

    } catch (IOException e) {
      e.printStackTrace();
    }

    return System.nanoTime() - start;
  }

  @Override
  public long flush() {
    return 0;
  }

  @Override
  public long close() {
    long start = System.nanoTime();
    long fileSize = 0;
    for (int i = 0, writersLength = writers.length; i < writersLength; i++) {
      Writer writer = writers[i];
      try {
        writer.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
      fileSize += new File(i + "_" + filePath).length();
    }
    logger.info("Total file size: {}", fileSize / (1024*1024.0));
    return System.nanoTime() - start;
  }
}

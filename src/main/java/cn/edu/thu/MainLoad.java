package cn.edu.thu;

import cn.edu.thu.common.BenchmarkExceptionHandler;
import cn.edu.thu.common.Config;
import cn.edu.thu.common.Statistics;
import cn.edu.thu.database.DatabaseFactory;
import cn.edu.thu.database.IDataBaseManager;
import cn.edu.thu.writer.RealDatasetWriter;
import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MainLoad {

  private static Logger logger = LoggerFactory.getLogger(MainLoad.class);

  public static void main(String[] args) throws Exception {
    long start = System.nanoTime();
    if (args == null || args.length == 0) {
      args = new String[]{"conf/config.properties"};
    }

    final Statistics statistics = new Statistics();

    Config config;
    if (args.length > 0) {
      try {
        FileInputStream fileInputStream = new FileInputStream(args[0]);
        config = new Config(fileInputStream);
      } catch (Exception e) {
        e.printStackTrace();
        logger.error("Load config from {} failed, using default config", args[0]);
        config = new Config();
      }
    } else {
      config = new Config();
    }

    // init database
    IDataBaseManager database = DatabaseFactory.getDbManager(config);
    database.initServer();

    logger.info("thread num : {}", config.THREAD_NUM);
    logger.info("using database: {}", config.DATABASE);

    File dirFile = new File(config.DATA_DIR);
    if (!dirFile.exists()) {
      logger.error(config.DATA_DIR + " do not exit");
      return;
    }

    List<String> files = new ArrayList<>();
    getAllFiles(config.DATA_DIR, files);
    logger.info("total files: {}", files.size());
    statistics.fileNum.addAndGet(files.size());

    Collections.sort(files);

    List<List<String>> thread_files = new ArrayList<>();
    for (int i = 0; i < config.THREAD_NUM; i++) {
      thread_files.add(new ArrayList<>());
    }

    for (int i = 0; i < files.size(); i++) {
      if (config.useSynthetic) {
        break;
      }

      if (i < config.BEGIN_FILE || i > config.END_FILE) {
        continue; // only load files numbered in [config.BEGIN_FILE, config.END_FILE]
      }

      String filePath = files.get(i);
      if (filePath.contains(".DS_Store")) {
        continue;
      }
      int thread = i % config.THREAD_NUM;
      thread_files.get(thread).add(filePath);
    }

    Thread.UncaughtExceptionHandler handler = new BenchmarkExceptionHandler();
    ExecutorService executorService = Executors.newFixedThreadPool(config.THREAD_NUM);
    for (int threadId = 0; threadId < config.THREAD_NUM; threadId++) {
      Thread thread = new Thread(
          new RealDatasetWriter(config, thread_files.get(threadId), statistics));
      thread.setUncaughtExceptionHandler(handler);
      executorService.submit(thread);
    }

    executorService.shutdown();
    logger.info("@+++<<<: shutdown thread pool");

    // wait for all threads done
    boolean allDown = false;
    while (!allDown) {
      if (executorService.isTerminated()) {
        allDown = true;
      }
      Thread.sleep(1000);
    }

    long sum = 0;
    for (int i = 0; i <= statistics.writeLatency.size() - 1; i++) {
      sum += statistics.writeLatency.get(i);
    }
    double average = sum * 1.0 / statistics.writeLatency.size() / 1000_000F;
    logger.info("Exp:{} All done! Total records:{}, points:{}, time:{} ms, speed:{} pts/s, "
            + "average latency:{} ms, "
            + "latency list(length:{},unit:ns):{}",
        config.EXP_NAME,
        statistics.recordNum,
        statistics.pointNum, (float) statistics.timeCost.get() / 1000_000F, statistics.speed(),
        average,
        statistics.writeLatency.size(),
        statistics.writeLatency
    );

    logger.info("total program running time: {} ms", (System.nanoTime() - start) / 1000_000F);
  }

  private static void getAllFiles(String strPath, List<String> files) {
    File f = new File(strPath);
    if (f.isDirectory()) {
      File[] fs = f.listFiles();
      for (File f1 : fs) {
        String fsPath = f1.getAbsolutePath();
        getAllFiles(fsPath, files);
      }
    } else if (f.isFile()) {
      files.add(f.getAbsolutePath());
    }
  }

}

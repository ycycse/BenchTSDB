package cn.edu.thu;

import cn.edu.thu.common.Config;
import cn.edu.thu.database.DatabaseFactory;
import cn.edu.thu.database.IDataBaseManager;
import java.io.FileInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MainQuery {

  private static Logger logger = LoggerFactory.getLogger(MainQuery.class);

  public static void main(String[] args) {

    if (args.length == 0) {
      args = new String[]{"conf/config.properties"};
    }

    Config config = null;
    try {
      FileInputStream fileInputStream = new FileInputStream(args[0]);
      config = new Config(fileInputStream);
    } catch (Exception e) {
      e.printStackTrace();
      logger.error("Load config from {} failed, using default config", args[0]);
      System.exit(1);
    }

    Config.FOR_QUERY = true;

    IDataBaseManager database = DatabaseFactory.getDbManager(config);
    database.initClient();

    long elapsedTime = database.query();
    logger.info("query time: {} ms", (float) elapsedTime / 1000_000F);
  }
}

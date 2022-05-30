package cn.edu.thu.common;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class QueryExpResultExtractTool {

  /**
   * [INFO ] 2022-05-27 17:35:02.584 [main] cn.edu.thu.database.timescaledb.TimescaleDBManager.query:158
   * - Query finished. Total lines: 135207. Query Number: 1
   * <p>
   * [INFO ] 2022-05-27 17:35:02.584 [main] cn.edu.thu.database.timescaledb.TimescaleDBManager.query:160
   * - SQL1: select time,"collecttime" from "root.T000100010002.90003_1" limit 1000000;
   * <p>
   * [INFO ] 2022-05-27 17:35:02.585 [main] cn.edu.thu.MainQuery.main:37 -
   * Exp:"TimescaleDB+SINGLE_SERIES_RAW_QUERY_1000000" done! Query Param:1000000. query time:
   * 200.31725 ms
   */
  public static void main(String[] args) throws Exception {

//    String file = "D:\\2\\TimescaleDB+SINGLE_SERIES_RAW_QUERY.txt";
//    String expStr = "TimescaleDB+SINGLE_SERIES_RAW_QUERY";
//    String output = expStr + ".csv";
//    int[] queryParameters = new int[]{1, 100, 10000, 100000, 1000000};

//    String file = "D:\\2\\TimescaleDB+MULTI_SERIES_ALIGN_QUERY.txt";
//    String expStr = "TimescaleDB+MULTI_SERIES_ALIGN_QUERY";
//    String output = expStr + ".csv";
//    int[] queryParameters = new int[]{1, 10, 100, 1000};

//    String file = "D:\\2\\TimescaleDB+SINGLE_SERIES_COUNT_QUERY.txt";
//    String expStr = "TimescaleDB+SINGLE_SERIES_COUNT_QUERY";
//    String output = expStr + ".csv";
//    int[] queryParameters = new int[]{1, 100, 10000, 1000000};

//    String file = "D:\\2\\TimescaleDB+SINGLE_SERIES_DOWNSAMPLING_QUERY.txt";
//    String expStr = "TimescaleDB+SINGLE_SERIES_DOWNSAMPLING_QUERY";
//    String output = expStr + ".csv";
//    int[] queryParameters = new int[]{1000000, 10000, 100, 1};

    // -------------------------------------------

//    String file = "D:\\2\\InfluxDB+SINGLE_SERIES_RAW_QUERY.txt";
//    String expStr = "InfluxDB+SINGLE_SERIES_RAW_QUERY";
//    String output = expStr + ".csv";
//    int[] queryParameters = new int[]{1, 100, 10000, 100000, 1000000};

//    String file = "D:\\2\\InfluxDB+MULTI_SERIES_ALIGN_QUERY.txt";
//    String expStr = "InfluxDB+MULTI_SERIES_ALIGN_QUERY";
//    String output = expStr + ".csv";
//    int[] queryParameters = new int[]{1, 10, 100, 1000};

//    String file = "D:\\2\\InfluxDB+SINGLE_SERIES_COUNT_QUERY.txt";
//    String expStr = "InfluxDB+SINGLE_SERIES_COUNT_QUERY";
//    String output = expStr + ".csv";
//    int[] queryParameters = new int[]{1, 100, 10000, 1000000};

//    String file = "D:\\2\\InfluxDB+SINGLE_SERIES_DOWNSAMPLING_QUERY.txt";
//    String expStr = "InfluxDB+SINGLE_SERIES_DOWNSAMPLING_QUERY";
//    String output = expStr + ".csv";
//    int[] queryParameters = new int[]{1, 100, 10000, 1000000};

    // -------------------------------------------

//    String file = "D:\\2\\IoTDB+align+SINGLE_SERIES_RAW_QUERY.txt";
//    String expStr = "IoTDB+SINGLE_SERIES_RAW_QUERY";
//    String output = "IoTDB+align+SINGLE_SERIES_RAW_QUERY.csv";
//    int[] queryParameters = new int[]{1, 100, 10000, 100000, 1000000};

//    String file = "D:\\2\\IoTDB+align+MULTI_SERIES_ALIGN_QUERY.txt";
//    String expStr = "IoTDB+MULTI_SERIES_ALIGN_QUERY";
//    String output = "IoTDB+align+MULTI_SERIES_ALIGN_QUERY.csv";
//    int[] queryParameters = new int[]{1, 10, 100, 1000};

//    String file = "D:\\2\\IoTDB+align+SINGLE_SERIES_COUNT_QUERY.txt";
//    String expStr = "IoTDB+SINGLE_SERIES_COUNT_QUERY";
//    String output = "IoTDB+align+SINGLE_SERIES_COUNT_QUERY.csv";
//    int[] queryParameters = new int[]{1, 100, 10000, 1000000};

//    String file = "D:\\2\\IoTDB+align+SINGLE_SERIES_DOWNSAMPLING_QUERY.txt";
//    String expStr = "IoTDB+SINGLE_SERIES_DOWNSAMPLING_QUERY";
//    String output = "IoTDB+align+SINGLE_SERIES_DOWNSAMPLING_QUERY.csv";
//    int[] queryParameters = new int[]{1, 100, 10000, 1000000};

    // -------------------------------------------

    String file = "D:\\2\\IoTDB+nonalign+SINGLE_SERIES_RAW_QUERY.txt";
    String expStr = "IoTDB+SINGLE_SERIES_RAW_QUERY";
    String output = "IoTDB+nonalign+SINGLE_SERIES_RAW_QUERY.csv";
    int[] queryParameters = new int[]{1, 100, 10000, 100000, 1000000};

//    String file = "D:\\2\\IoTDB+nonalign+MULTI_SERIES_ALIGN_QUERY.txt";
//    String expStr = "IoTDB+MULTI_SERIES_ALIGN_QUERY";
//    String output = "IoTDB+nonalign+MULTI_SERIES_ALIGN_QUERY.csv";
//    int[] queryParameters = new int[]{1, 10, 100, 1000};

//    String file = "D:\\2\\IoTDB+nonalign+SINGLE_SERIES_COUNT_QUERY.txt";
//    String expStr = "IoTDB+SINGLE_SERIES_COUNT_QUERY";
//    String output = "IoTDB+nonalign+SINGLE_SERIES_COUNT_QUERY.csv";
//    int[] queryParameters = new int[]{1, 100, 10000, 1000000};

//    String file = "D:\\2\\IoTDB+nonalign+SINGLE_SERIES_DOWNSAMPLING_QUERY.txt";
//    String expStr = "IoTDB+SINGLE_SERIES_DOWNSAMPLING_QUERY";
//    String output = "IoTDB+nonalign+SINGLE_SERIES_DOWNSAMPLING_QUERY.csv";
//    int[] queryParameters = new int[]{1, 100, 10000, 1000000};

    // -------------------------------------------

//    String file = "D:\\2\\KairosDB+SINGLE_SERIES_RAW_QUERY.txt";
//    String expStr = "KairosDB+SINGLE_SERIES_RAW_QUERY";
//    String output = expStr + ".csv";
//    int[] queryParameters = new int[]{1, 100, 10000, 100000, 1000000};

//    String file = "D:\\2\\KairosDB+MULTI_SERIES_ALIGN_QUERY.txt";
//    String expStr = "KairosDB+MULTI_SERIES_ALIGN_QUERY";
//    String output = expStr + ".csv";
//    int[] queryParameters = new int[]{1, 10, 100, 1000};

//    String file = "D:\\2\\KairosDB+SINGLE_SERIES_COUNT_QUERY.txt";
//    String expStr = "KairosDB+SINGLE_SERIES_COUNT_QUERY";
//    String output = expStr + ".csv";
//    int[] queryParameters = new int[]{1, 100, 10000, 1000000};

//    String file = "D:\\2\\KairosDB+SINGLE_SERIES_DOWNSAMPLING_QUERY.txt";
//    String expStr = "KairosDB+SINGLE_SERIES_DOWNSAMPLING_QUERY";
//    String output = expStr + ".csv";
//    int[] queryParameters = new int[]{1, 100, 10000, 1000000};

    String expNameFormat = "\"%s_%d\""; // the double quote is important to distinguish different parameters
    int repeatNumber = 5; // 每个查询参数的实验重复次数

    Map<Integer, List<Double>> queryLatencies = new HashMap<>();
    for (int param : queryParameters) {
      queryLatencies.put(param, new ArrayList<>());
    }
    BufferedReader reader = new BufferedReader(new FileReader(file));
    PrintWriter writer = new PrintWriter(new FileOutputStream(new File(output)));
    String line;
    while ((line = reader.readLine()) != null) {
      for (int param : queryParameters) {
        String expName = String.format(expNameFormat, expStr, param);
        if (line.contains(expName)) {
          // for example: "...query time: 17.366323 ms..."
          // [INFO ] 2022-05-27 17:33:43.182 [main] cn.edu.thu.MainQuery.main:37 - Exp:"TimescaleDB+SINGLE_SERIES_RAW_QUERY_1" done! Query Param:1. query time: 17.366323 ms
          String[] splits = line.split("\\s+");
          String latencyStr = splits[splits.length - 2]; //倒数第二项
          double latency = Double.parseDouble(latencyStr);
          queryLatencies.get(param).add(latency);
          break;
        }
      }
    }

    StringBuilder header = new StringBuilder();
    header.append("queryParameter");
    for (int i = 1; i <= repeatNumber; i++) {
      header.append(",");
      header.append("repeat" + i);
    }
    header.append(",");
    header.append("average(ms)");
    writer.println(header.toString());
    for (int param : queryParameters) {
      double sum = 0;
      int actualRepeatNum = 0;
      StringBuilder stringBuilder = new StringBuilder();
      stringBuilder.append(param);
      for (double latency : queryLatencies.get(param)) {
        sum += latency;
        actualRepeatNum++;
        stringBuilder.append(",");
        stringBuilder.append(latency);
      }
      stringBuilder.append(",");
      stringBuilder.append(sum / actualRepeatNum);
      // each line corresponds to the repeated query latencies under a query parameter
      writer.println(stringBuilder.toString());
    }

    reader.close();
    writer.close();
  }
}
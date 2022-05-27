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

  public static void main(String[] args) throws Exception {
    String file = "D:\\2\\bench.log";
    String expStr = "TimescaleDB+SINGLE_SERIES_RAW_QUERY";
    String expNameFormat = "\"%s_%d\""; // the double quote is important to distinguish different parameters
    int[] queryParameters = new int[]{1, 100, 10000, 1000000};
    int repeatNumber = 5; // 每个查询参数的实验重复次数
    String output = expStr + ".csv";

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
      StringBuilder stringBuilder = new StringBuilder();
      stringBuilder.append(param);
      for (double latency : queryLatencies.get(param)) {
        sum += latency;
        stringBuilder.append(",");
        stringBuilder.append(latency);
      }
      stringBuilder.append(",");
      stringBuilder.append(sum / repeatNumber);
      // each line corresponds to the repeated query latencies under a query parameter
      writer.println(stringBuilder.toString());
    }

    reader.close();
    writer.close();
  }
}
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package cn.edu.thu.reader;

import cn.edu.thu.common.Config;
import cn.edu.thu.common.IndexedSchema;
import cn.edu.thu.common.IndexedSchema.MapIndexedSchema;
import cn.edu.thu.common.Record;
import cn.edu.thu.common.Schema;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CSVReader extends BasicReader {

  private static final Logger logger = LoggerFactory.getLogger(CSVReader.class);

  private final String separator = ",";
  private final int defaultPrecision = 8;
  private IndexedSchema overallSchema;
  private Schema currentFileSchema;

  public CSVReader(Config config, List<String> files) {
    super(config, files);
    if (!config.splitFileByDevice) {
      overallSchema = collectSchemaFromFiles(files);
    }
  }

  private IndexedSchema collectSchemaFromFiles(List<String> files) {
    SchemaSet schemaSet = new SchemaSet();
    for (String file : files) {
      schemaSet.union(collectSchemaFromFile(file));
    }
    return schemaSet.toSchema();
  }

  private Schema collectSchemaFromFile(String file) {
    try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
      String headerLine = reader.readLine();
      if (headerLine != null) {
        return convertHeaderToSchema(headerLine);
      }
    } catch (IOException e) {
      logger.warn("Cannot read schema from file {}, file skipped", file);
    }

    return null;
  }

  private Schema convertHeaderToSchema(String headerLine) {
    String[] split = headerLine.split(separator);
    Schema schema = new Schema();

    // the first field is fixed to time
    int fieldNum = split.length - 1;
    schema.fields = new String[fieldNum];
    schema.precision = new int[fieldNum];

    int devicePos = split[1].lastIndexOf('.');
    schema.tag = split[1].substring(0, devicePos);
    for (int i = 1; i < split.length; i++) {
      String seriesName = split[i];
      String measurement = seriesName.substring(devicePos + 1);
      schema.fields[i - 1] = measurement;
      schema.precision[i - 1] = defaultPrecision;
    }

    return schema;
  }

  @Override
  public List<Record> convertCachedLinesToRecords() {
    List<Record> records = new ArrayList<>();
    for (String cachedLine : cachedLines) {
      records.add(convertToRecord(cachedLine));
    }
    return records;
  }

  private List<Object> fieldsWithCurrentFileSchema(String[] split) {
    List<Object> fields = new ArrayList<>(currentFileSchema.fields.length);
    for (int i = 1; i < split.length; i++) {
      if (split[i].isEmpty() || split[i].equalsIgnoreCase("null")) {
        fields.add(null);
      } else {
        fields.add(Double.parseDouble(split[i]));
      }
    }
    return fields;
  }

  private List<Object> fieldsWithOverallSchema(String[] split) {
    List<Object> fields = new ArrayList<>(overallSchema.fields.length);
    for (int i = 0; i < overallSchema.fields.length; i++) {
      fields.add(null);
    }

    for (int i = 0; i < currentFileSchema.fields.length; i++) {
      int overallIndex = overallSchema.getIndex(currentFileSchema.fields[i]);
      if (split[i].isEmpty() || split[i].equalsIgnoreCase("null")) {
        fields.set(overallIndex, null);
      } else {
        fields.set(overallIndex, Double.parseDouble(split[i]));
      }
    }
    return fields;
  }


  private Record convertToRecord(String line) {
    Record record;
    String[] split = line.split(separator);
    long time = Long.parseLong(split[0]);
    String tag = currentFileSchema.tag;
    List<Object> fields;

    if (config.splitFileByDevice) {
      fields = fieldsWithCurrentFileSchema(split);
    } else {
      fields = fieldsWithOverallSchema(split);
    }
    record = new Record(time, tag, fields);

    return record;
  }

  @Override
  public void onFileOpened() {
    Schema fileSchema;
    try {
      fileSchema = convertHeaderToSchema(reader.readLine());
    } catch (IOException e) {
      logger.warn("Cannot read schema from {}, skipping", currentFile);
      return;
    }
    currentFileSchema = fileSchema;
  }

  @Override
  public Schema getCurrentSchema() {
    return config.splitFileByDevice ? currentFileSchema : overallSchema;
  }

  private static class SchemaSet {
    private final Map<String, Integer> fieldPrecisionMap = new HashMap<>();

    public void union(Schema schema) {
      if (schema == null) {
        return;
      }

      for (int i = 0; i < schema.fields.length; i++) {
        int finalI = i;
        fieldPrecisionMap.compute(schema.fields[i], (s, p) -> Math.max(p == null ? 0 : p,
            schema.precision[finalI]));
      }
    }

    public IndexedSchema toSchema() {
      MapIndexedSchema schema = new MapIndexedSchema();
      schema.fields = new String[fieldPrecisionMap.size()];
      schema.precision = new int[fieldPrecisionMap.size()];

      int index = 0;
      for (Entry<String, Integer> entry : fieldPrecisionMap.entrySet()) {
        schema.fields[index] = entry.getKey();
        schema.precision[index ++] = entry.getValue();
      }

      return schema.rebuildIndex();
    }
  }
}

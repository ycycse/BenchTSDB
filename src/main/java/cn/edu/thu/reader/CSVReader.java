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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
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
        return convertHeaderToSchema(headerLine, reader);
      }
    } catch (IOException e) {
      logger.warn("Cannot read schema from file {}, file skipped", file);
    }

    return null;
  }

  private void inferTypeWithData(int fieldNum, Schema schema) throws IOException {
    Set<Integer> unknownTypeIndices = new HashSet<>();
    for (int i = 0; i < fieldNum; i++) {
      unknownTypeIndices.add(i);
    }

    String line;
    List<Integer> indexToRemove = new ArrayList<>();
    while ((line = reader.readLine()) != null
        && !unknownTypeIndices.isEmpty()
        && cachedLines.size() < config.BATCH_SIZE) {
      String[] lineSplit = line.split(separator);
      indexToRemove.clear();

      for (Integer unknownTypeIndex : unknownTypeIndices) {
        String field = removeQuote(lineSplit[unknownTypeIndex + 1]);
        Class<?> aClass = inferType(field);
        if (aClass != null) {
          schema.getTypes()[unknownTypeIndex] = aClass;
          indexToRemove.add(unknownTypeIndex);
        }
      }

      unknownTypeIndices.removeAll(indexToRemove);
      cachedLines.add(line);
    }

    // if some fields cannot be inferred within a batch, assume them as text
    for (Integer unknownTypeIndex : unknownTypeIndices) {
      schema.getTypes()[unknownTypeIndex] = String.class;
    }
  }

  private Schema convertHeaderToSchema(String headerLine, BufferedReader reader)
      throws IOException {
    String[] split = headerLine.split(separator);
    Schema schema = new Schema();

    // the first field is fixed to time
    int fieldNum = split.length - 1;
    schema.setFields(new String[fieldNum]);
    schema.setPrecision(new int[fieldNum]);

    int devicePos = split[1].lastIndexOf('.');
    schema.setTag(split[1].substring(0, devicePos));
    for (int i = 1; i < split.length; i++) {
      String seriesName = split[i];
      String measurement = seriesName.substring(devicePos + 1);
      schema.getFields()[i - 1] = measurement;
      schema.getPrecision()[i - 1] = defaultPrecision;
    }

    // infer datatype using at most a batch of lines
    if (overallSchema == null) {
      inferTypeWithData(fieldNum, schema);
    } else {
      inferTypeWithOverallSchema(schema);
    }

    return schema;
  }

  private void inferTypeWithOverallSchema(Schema schema) {
    for (int i = 0; i < schema.getFields().length; i++) {
      String field = schema.getFields()[i];
      schema.getTypes()[i] = overallSchema.getTypes()[overallSchema.getIndex(field)];
    }
  }

  private Class<?> inferType(String field) {
    if (field.equalsIgnoreCase("null")) {
      return null;
    }

    try {
      Long.parseLong(field);
      return Long.class;
    } catch (NumberFormatException ignore) {
      // ignored
    }

    try {
      Double.parseDouble(field);
      return Double.class;
    } catch (NumberFormatException ignore) {
      // ignored
    }

    return String.class;
  }

  @Override
  public List<Record> convertCachedLinesToRecords() {
    List<Record> records = new ArrayList<>();
    for (String cachedLine : cachedLines) {
      records.add(convertToRecord(cachedLine));
    }
    return records;
  }

  private String removeQuote(String s) {
    if (s.length() >= 2 &&
        s.startsWith("'") && s.endsWith("'") ||
        s.startsWith("\"") && s.endsWith("\"")) {
      return s.substring(1, s.length() - 1);
    }
    return s;
  }

  private Object parseField(String field, Schema schema, int index) {
    if (field.isEmpty() || field.equalsIgnoreCase("null")) {
      return null;
    }

    Class<?> type = schema.getTypes()[index];
    if (type == Long.class) {
      return Long.parseLong(field);
    }
    if (type == Double.class) {
      return Double.parseDouble(field);
    }
    return field;
  }

  private List<Object> fieldsWithCurrentFileSchema(String[] split) {
    List<Object> fields = new ArrayList<>(currentFileSchema.getFields().length);
    for (int i = 1; i < split.length; i++) {
      split[i] = removeQuote(split[i]);

      fields.add(parseField(split[i], currentFileSchema, i - 1));
    }
    return fields;
  }

  private List<Object> fieldsWithOverallSchema(String[] split) {
    List<Object> fields = new ArrayList<>(overallSchema.getFields().length);
    for (int i = 0; i < overallSchema.getFields().length; i++) {
      fields.add(null);
    }

    for (int i = 1; i < split.length; i++) {
      int overallIndex = overallSchema.getIndex(currentFileSchema.getFields()[i]);
      split[i] = removeQuote(split[i]);

      fields.set(overallIndex, parseField(split[i], overallSchema, overallIndex));
    }
    return fields;
  }


  private Record convertToRecord(String line) {
    Record record;
    String[] split = line.split(separator);
    long time = Long.parseLong(split[0]);
    String tag = currentFileSchema.getTag();
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
      fileSchema = convertHeaderToSchema(reader.readLine(), reader);
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
    private final Map<String, Class<?>> fieldTypeMap = new HashMap<>();

    public void union(Schema schema) {
      if (schema == null) {
        return;
      }

      for (int i = 0; i < schema.getFields().length; i++) {
        int finalI = i;
        fieldPrecisionMap.compute(schema.getFields()[i], (s, p) -> Math.max(p == null ? 0 : p,
            schema.getPrecision()[finalI]));
        fieldTypeMap.compute(schema.getFields()[i], (s, t) -> {
          Class<?> newType = schema.getTypes()[schema.getIndex(s)];
          return mergeType(t, newType);
        });
      }
    }

    private Class<?> mergeType(Class<?> t1, Class<?> t2) {
      if (t1 == null && t2 == null) {
        return null;
      }
      if (t1 == null) {
        return t2;
      }
      if (t2 ==null) {
        return t1;
      }

      if (t1 != t2) {
        return String.class;
      }
      return t1;
    }

    public IndexedSchema toSchema() {
      MapIndexedSchema schema = new MapIndexedSchema();
      schema.setFields(new String[fieldPrecisionMap.size()]);
      schema.setPrecision(new int[fieldPrecisionMap.size()]);

      int index = 0;
      for (Entry<String, Integer> entry : fieldPrecisionMap.entrySet()) {
        schema.getFields()[index] = entry.getKey();
        schema.getTypes()[index] = fieldTypeMap.getOrDefault(entry.getKey(), String.class);
        schema.getPrecision()[index++] = entry.getValue();
      }

      return schema.rebuildIndex();
    }
  }
}

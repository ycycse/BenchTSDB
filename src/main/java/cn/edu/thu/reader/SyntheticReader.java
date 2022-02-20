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
import cn.edu.thu.common.Record;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class SyntheticReader extends BasicReader {

  private int deviceCursor = 0;
  private int pointCursor = 0;
  private Random random = new Random(123456);

  public SyntheticReader(Config config) {
    super(config);
  }

  @Override
  public boolean hasNextBatch() {
    return deviceCursor < config.syntheticDeviceNum;
  }

  @Override
  public List<Record> nextBatch() {
    List<Record> batch = new ArrayList<>(config.BATCH_SIZE);
    String device = "root.device_" + deviceCursor;
    List<Object> fields = new ArrayList<>(config.syntheticMeasurementNum);
    for (int i = 0; i < config.BATCH_SIZE && pointCursor < config.syntheticPointNum; i++) {
      fields.clear();
      for (int j = 0; j < config.syntheticMeasurementNum; j++) {
        fields.add(random.nextDouble() < config.syntheticNullRatio ? null : random.nextDouble());
      }
      Record record = new Record(pointCursor++, device, fields);
      batch.add(record);
    }
    if (pointCursor == config.syntheticPointNum) {
      deviceCursor ++;
      pointCursor = 0;
    }
    return batch;
  }

  @Override
  public void init() throws Exception {
     // no action
  }
}

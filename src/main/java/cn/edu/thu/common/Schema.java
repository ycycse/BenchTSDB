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

package cn.edu.thu.common;

import java.util.Arrays;
import java.util.Objects;

public class Schema {
  public String[] fields = null;
  public int[] precision = null;
  // optional, if the schema is bound to a specify tag
  public String tag = Config.DEFAULT_TAG;

  public Schema() {
  }

  public Schema(String[] fields, int[] precision) {
    this.fields = fields;
    this.precision = precision;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Schema schema = (Schema) o;
    return Arrays.equals(fields, schema.fields) &&
        Arrays.equals(precision, schema.precision) &&
        Objects.equals(tag, schema.tag);
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(tag);
    result = 31 * result + Arrays.hashCode(fields);
    result = 31 * result + Arrays.hashCode(precision);
    return result;
  }
}

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
  private String[] fields = null;
  private int[] precision = null;
  private Class<?>[] types = null;
  // optional, if the schema is bound to a specify tag
  private String tag = Config.DEFAULT_TAG;

  public int getIndex(String fieldName) {
    for (int i = 0; i < fields.length; i++) {
      if (fieldName.equals(fields[i])) {
        return i;
      }
    }
    return -1;
  }

  public Schema() {
  }

  public Schema(String[] fields, int[] precision) {
    this.setFields(fields);
    this.setPrecision(precision);
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
    return Arrays.equals(getFields(), schema.getFields()) &&
        Arrays.equals(getPrecision(), schema.getPrecision()) &&
        Objects.equals(getTag(), schema.getTag());
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(getTag());
    result = 31 * result + Arrays.hashCode(getFields());
    result = 31 * result + Arrays.hashCode(getPrecision());
    return result;
  }

  public String[] getFields() {
    return fields;
  }

  public void setFields(String[] fields) {
    this.fields = fields;
    this.types = new Class[fields.length];
  }

  public int[] getPrecision() {
    return precision;
  }

  public void setPrecision(int[] precision) {
    this.precision = precision;
  }

  public String getTag() {
    return tag;
  }

  public void setTag(String tag) {
    this.tag = tag;
  }

  public Class<?>[] getTypes() {
    return types;
  }
}

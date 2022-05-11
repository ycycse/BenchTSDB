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

import java.io.File;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Stack;

public class SuffixFileIterator implements Iterator<File> {

  private String suffix;
  private Stack<File> fileStack = new Stack<>();
  private File current;

  public SuffixFileIterator(List<String> initialFilePaths, String suffix) {
    initialFilePaths.stream().map(File::new).filter(File::exists)
        .forEach(f -> fileStack.push(f));
    this.suffix = suffix;
  }

  @Override
  public boolean hasNext() {
    if (current != null) {
      return true;
    }

    if (!fileStack.empty()) {
      nextInStack();
    }

    return current != null;
  }

  private void nextInStack() {
    while (!fileStack.empty()) {
      File pop = fileStack.pop();
      if (pop.isDirectory()) {
        File[] subFiles = pop.listFiles(s -> s.isDirectory() || s.getName().endsWith(suffix));
        if (subFiles != null) {
          fileStack.addAll(Arrays.asList(subFiles));
        }
      } else if (pop.getName().endsWith(suffix)) {
        current = pop;
        return;
      }
    }
  }


  @Override
  public File next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    return current;
  }
}

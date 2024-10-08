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

package cn.edu.thu.database.sqlite;

import cn.edu.thu.common.Record;
import cn.edu.thu.common.Schema;
import cn.edu.thu.database.IDataBaseManager;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class SqlLiteManager implements IDataBaseManager {
    private Connection sqliteConnection;
    private String pathToDB;
    @Override
    public long insertBatch(List<Record> records, Schema schema) {
        long startTime = 0;
        try (Statement stmt = sqliteConnection.createStatement()){
            for(Record record:records){
                String insertSql = getInsertSql(schema, record.timestamp, record.fields);
                stmt.addBatch(insertSql);
            }
            startTime = System.nanoTime();
            stmt.executeBatch();
        } catch (Exception e){
            e.printStackTrace();
        }
        return System.nanoTime() - startTime;
    }

    private String typeMap(Class<?> sensorType) {
        if (sensorType == Long.class) {
            return "BIGINT";
        } else if (sensorType == Double.class) {
            return "DOUBLE PRECISION";
        } else if (sensorType == String.class) {
            return "TEXT";
        } else {
           return "TEXT";
        }
    }

    private void registerSchema(Schema schema){
        try (Statement statement = sqliteConnection.createStatement()){
            StringBuilder builder = new StringBuilder("CREATE TABLE ").append(encapName(schema.getTag())).append(" (");
            // time
            for(int i=0;i<schema.getFields().length;i++){
                builder
                        .append(", ")
                        .append(encapName(schema.getFields()[i]))
                        .append(" ")
                        .append(typeMap(schema.getTypes()[i]));
            }
            builder.append(");");
            statement.execute(builder.toString());
        } catch (Exception e){
            e.printStackTrace();
        }
    }

    private String getInsertSql(Schema schema, long timestamp, List<Object> values) {
        ArrayList<String> sqls = new ArrayList<>();
        StringBuilder builder = new StringBuilder();
        builder.append("insert into ")
                .append(encapName(schema.getTag())).append("(time");

        StringBuilder schemaBuilder = new StringBuilder();
        StringBuilder valueBuilder = new StringBuilder();
        for(int i=0;i<schema.getFields().length;i++){
            schemaBuilder.append(",").append(encapName(schema.getFields()[i]));

            Object value = values.get(i);
            if (schema.getTypes()[i] == String.class && value != null) {
                value = ((String) value).replace('\'', '\"');
                valueBuilder.append(",'").append(value).append("'");
               } else {
                valueBuilder.append(",").append(value);
            }
        }

        builder.append(schemaBuilder);
        builder.append(") values(");
        builder.append(timestamp);
        builder.append(valueBuilder);
        builder.append(");");
        return builder.toString();
    }

    private String encapName(String name) {
        return "\"" + name + "\""; // to escape special characters like -,@,.
    }

    @Override
    public void initServer() {
        try {
            Class.forName("org.sqlite.JDBC");
            sqliteConnection = DriverManager.getConnection("jdbc:sqlite:/" + pathToDB);
        }catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void initClient() {

    }

    @Override
    public long query() {
        return 0;
    }

    @Override
    public long flush() {
        return 0;
    }

    @Override
    public long close() {
        try {
            sqliteConnection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return 0;
    }
}

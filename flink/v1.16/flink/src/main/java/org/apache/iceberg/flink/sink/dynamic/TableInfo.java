/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *   http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package org.apache.iceberg.flink.sink.dynamic;

/**
 * 表信息
 *
 * @author TangFD 2023/03/16
 */
public class TableInfo {
    private String db;
    private String table;

    private TableInfo() {
    }

    public TableInfo(String db, String table) {
        this.db = db;
        this.table = table;
    }

    public static TableInfo of(String db, String tableName) {
        return new TableInfo(db, tableName);
    }

    public static TableInfo of(String tableName) {
        return of(null, tableName);
    }

    public String getDb() {
        return db;
    }

    public String getTable() {
        return table;
    }
}

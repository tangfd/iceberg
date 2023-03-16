/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.flink.sink.dynamic;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.flink.sink.PartitionKeySelector;
import org.apache.iceberg.flink.sink.dynamic.table.IcebergTableServiceLoader;

/**
 * Create a {@link PartitionKeySelector} to shuffle by partition key, then each partition/bucket will be wrote by only
 * one task. That will reduce lots of small files in partitioned fanout write policy for {@link FlinkSink}.
 */
class DynamicPartitionKeySelector implements KeySelector<RowDataWithTable, String> {
    // TODO: 2023/3/16 换成 Caffeine 设置淘汰
    private static final transient Map<String, PartitionKeySelector> KEY_SELECTOR_MAP = new ConcurrentHashMap<>(256);

    @Override
    public String getKey(RowDataWithTable row) {
        TableInfo tableInfo = row.tableInfo;
        return KEY_SELECTOR_MAP.computeIfAbsent(tableInfo.getTable(), t -> create(tableInfo)).getKey(row.rowData);
    }

    private PartitionKeySelector create(TableInfo tableInfo) {
        Table table = IcebergTableServiceLoader.loadTable(tableInfo);
        Schema schema = table.schema();
        TableSchema tableSchema = FlinkSchemaUtil.toSchema(schema);
        RowType flinkRowType = FlinkDynamicTableSink.toFlinkRowType(schema, tableSchema);
        return new PartitionKeySelector(table.spec(), schema, flinkRowType);
    }
}

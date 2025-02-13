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

import com.github.benmanes.caffeine.cache.Cache;
import java.util.List;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.sink.EqualityFieldKeySelector;
import org.apache.iceberg.flink.sink.dynamic.table.IcebergTableServiceLoader;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

/**
 * Create a {@link EqualityFieldKeySelector} to shuffle by equality fields, to ensure same equality fields record will
 * be emitted to same writer in order.
 */
class DynamicEqualityFieldKeySelector implements KeySelector<RowDataWithTable, Integer> {
    private static final transient Cache<String, EqualityFieldKeySelector> KEY_SELECTOR_CACHE = CacheUtils.createCache();
    private final ParameterTool param;

    public DynamicEqualityFieldKeySelector(ParameterTool param) {
        this.param = param;
    }

    @Override
    public Integer getKey(RowDataWithTable row) {
        TableInfo tableInfo = row.tableInfo;
        return KEY_SELECTOR_CACHE.get(tableInfo.getTable(), t -> create(tableInfo)).getKey(row.rowData);
    }

    private EqualityFieldKeySelector create(TableInfo tableInfo) {
        Table table = IcebergTableServiceLoader.loadTable(tableInfo, param);
        Schema schema = table.schema();
        TableSchema tableSchema = FlinkSchemaUtil.toSchema(schema);
        RowType flinkRowType = FlinkDynamicTableSink.toFlinkRowType(schema, tableSchema);
        List<Integer> equalityFieldIds = Lists.newArrayList(table.schema().identifierFieldIds());
        return new EqualityFieldKeySelector(schema, flinkRowType, equalityFieldIds);
    }
}

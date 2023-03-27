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
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.FlinkWriteConf;
import org.apache.iceberg.flink.FlinkWriteOptions;
import org.apache.iceberg.flink.sink.RowDataTaskWriterFactory;
import org.apache.iceberg.flink.sink.dynamic.table.IcebergTableServiceLoader;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

import static org.apache.iceberg.TableProperties.AVRO_COMPRESSION;
import static org.apache.iceberg.TableProperties.AVRO_COMPRESSION_LEVEL;
import static org.apache.iceberg.TableProperties.ORC_COMPRESSION;
import static org.apache.iceberg.TableProperties.ORC_COMPRESSION_STRATEGY;
import static org.apache.iceberg.TableProperties.PARQUET_COMPRESSION;
import static org.apache.iceberg.TableProperties.PARQUET_COMPRESSION_LEVEL;

/**
 * 动态 RowDataTaskWriterFactory
 *
 * @author TangFD 2023/03/16
 */
public class DynamicRowDataTaskWriterFactory implements DynamicTaskWriterFactory<RowData> {

    // TODO: 2023/3/16 换成 Caffeine 设置淘汰
    private static final transient Map<String, RowDataTaskWriterFactory> FACTORY_MAP = new ConcurrentHashMap<>(256);

    private final boolean upsert;
    private final ParameterTool param;
    private final Map<String, String> writeOptions;
    private final ReadableConfig readableConfig;

    public DynamicRowDataTaskWriterFactory(Map<String, String> writeOptions, ReadableConfig readableConfig, ParameterTool param) {
        this.writeOptions = writeOptions;
        this.readableConfig = readableConfig;
        this.upsert = Boolean.parseBoolean(writeOptions.get(FlinkWriteOptions.WRITE_UPSERT_ENABLED.key()));
        this.param = param;
    }

    @Override
    public TaskWriter<RowData> create(TableInfo tableInfo, int taskId, int attemptId) {
        return FACTORY_MAP.computeIfAbsent(tableInfo.getTable(),
                t -> createTaskWriterFactory(tableInfo, taskId, attemptId)).create();
    }

    private RowDataTaskWriterFactory createTaskWriterFactory(TableInfo tableInfo, int taskId, int attemptId) {
        Table table = IcebergTableServiceLoader.loadTable(tableInfo, param);
        FlinkWriteConf flinkWriteConf = new FlinkWriteConf(table, writeOptions, readableConfig);
        FileFormat format = flinkWriteConf.dataFileFormat();
        Schema schema = table.schema();
        TableSchema tableSchema = FlinkSchemaUtil.toSchema(schema);
        RowDataTaskWriterFactory writerFactory = new RowDataTaskWriterFactory(
                table,
                FlinkDynamicTableSink.toFlinkRowType(schema, tableSchema),
                flinkWriteConf.targetDataFileSize(),
                format,
                writeProperties(table, format, flinkWriteConf),
                Lists.newArrayList(schema.identifierFieldIds()),
                upsert);
        writerFactory.initialize(taskId, attemptId);
        return writerFactory;
    }

    /**
     * Based on the {@link FileFormat} overwrites the table level compression properties for the table write.
     *
     * @param table  The table to get the table level settings
     * @param format The FileFormat to use
     * @param conf   The write configuration
     * @return The properties to use for writing
     */
    private Map<String, String> writeProperties(Table table, FileFormat format, FlinkWriteConf conf) {
        Map<String, String> writeProperties = Maps.newHashMap(table.properties());
        switch (format) {
            case PARQUET:
                writeProperties.put(PARQUET_COMPRESSION, conf.parquetCompressionCodec());
                String parquetCompressionLevel = conf.parquetCompressionLevel();
                if (parquetCompressionLevel != null) {
                    writeProperties.put(PARQUET_COMPRESSION_LEVEL, parquetCompressionLevel);
                }

                break;
            case AVRO:
                writeProperties.put(AVRO_COMPRESSION, conf.avroCompressionCodec());
                String avroCompressionLevel = conf.avroCompressionLevel();
                if (avroCompressionLevel != null) {
                    writeProperties.put(AVRO_COMPRESSION_LEVEL, conf.avroCompressionLevel());
                }

                break;
            case ORC:
                writeProperties.put(ORC_COMPRESSION, conf.orcCompressionCodec());
                writeProperties.put(ORC_COMPRESSION_STRATEGY, conf.orcCompressionStrategy());
                break;
            default:
                throw new IllegalArgumentException(String.format("Unknown file format %s", format));
        }

        return writeProperties;
    }
}

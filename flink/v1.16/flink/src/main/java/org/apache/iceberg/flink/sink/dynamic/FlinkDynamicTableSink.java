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

import java.util.Map;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.Schema;
import org.apache.iceberg.flink.FlinkConfigOptions;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.FlinkWriteOptions;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.TypeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.iceberg.TableProperties.WRITE_DISTRIBUTION_MODE;

/**
 * 动态表 sink
 *
 * @author TangFD 2023/03/16
 */
public class FlinkDynamicTableSink {
    private static final Logger LOG = LoggerFactory.getLogger(FlinkDynamicTableSink.class);

    private static final String ICEBERG_STREAM_WRITER_NAME = DynamicIcebergStreamWriter.class.getSimpleName();
    private static final String ICEBERG_FILES_COMMITTER_NAME = DynamicIcebergFilesCommitter.class.getSimpleName();

    private FlinkDynamicTableSink() {
    }

    /**
     * Initialize a {@link Builder} to export the data from input data stream with {@link RowDataWithTable}s into
     * iceberg table.
     *
     * @param input the source input data stream with {@link RowDataWithTable}s.
     * @return {@link Builder} to connect the iceberg table.
     */
    public static Builder forRowData(DataStream<RowDataWithTable> input) {
        return new Builder().forRowData(input);
    }

    public static class Builder {
        private DataStream<RowDataWithTable> rowDataInput = null;
        private Integer writeParallelism = null;
        private final Map<String, String> snapshotProperties = Maps.newHashMap();
        private ReadableConfig readableConfig = new Configuration();
        private final Map<String, String> writeOptions = Maps.newHashMap();

        private Builder() {
        }

        private Builder forRowData(DataStream<RowDataWithTable> newRowDataInput) {
            this.rowDataInput = newRowDataInput;
            return this;
        }

        /**
         * Set the write properties for Flink sink. View the supported properties in {@link FlinkWriteOptions}
         */
        public Builder set(String property, String value) {
            writeOptions.put(property, value);
            return this;
        }

        /**
         * Set the write properties for Flink sink. View the supported properties in {@link FlinkWriteOptions}
         */
        public Builder setAll(Map<String, String> properties) {
            writeOptions.putAll(properties);
            return this;
        }

        public Builder overwrite(boolean newOverwrite) {
            writeOptions.put(FlinkWriteOptions.OVERWRITE_MODE.key(), Boolean.toString(newOverwrite));
            return this;
        }

        public Builder flinkConf(ReadableConfig config) {
            this.readableConfig = config;
            return this;
        }

        /**
         * Configure the write {@link DistributionMode} that the flink sink will use. Currently, flink support {@link
         * DistributionMode#NONE} and {@link DistributionMode#HASH}.
         *
         * @param mode to specify the write distribution mode.
         * @return {@link Builder} to connect the iceberg table.
         */
        public Builder distributionMode(DistributionMode mode) {
            Preconditions.checkArgument(
                    !DistributionMode.RANGE.equals(mode),
                    "Flink does not support 'range' write distribution mode now.");
            if (mode != null) {
                writeOptions.put(FlinkWriteOptions.DISTRIBUTION_MODE.key(), mode.modeName());
            }
            return this;
        }

        /**
         * Configuring the write parallel number for iceberg stream writer.
         *
         * @param newWriteParallelism the number of parallel iceberg stream writer.
         * @return {@link Builder} to connect the iceberg table.
         */
        public Builder writeParallelism(int newWriteParallelism) {
            this.writeParallelism = newWriteParallelism;
            return this;
        }

        /**
         * All INSERT/UPDATE_AFTER events from input stream will be transformed to UPSERT events, which means it will
         * DELETE the old records and then INSERT the new records. In partitioned table, the partition fields should be
         * a subset of equality fields, otherwise the old row that located in partition-A could not be deleted by the
         * new row that located in partition-B.
         *
         * @param enabled indicate whether it should transform all INSERT/UPDATE_AFTER events to UPSERT.
         * @return {@link Builder} to connect the iceberg table.
         */
        public Builder upsert(boolean enabled) {
            writeOptions.put(FlinkWriteOptions.WRITE_UPSERT_ENABLED.key(), Boolean.toString(enabled));
            return this;
        }

        public Builder setSnapshotProperties(Map<String, String> properties) {
            snapshotProperties.putAll(properties);
            return this;
        }

        public Builder setSnapshotProperty(String property, String value) {
            snapshotProperties.put(property, value);
            return this;
        }

        public Builder toBranch(String branch) {
            writeOptions.put(FlinkWriteOptions.BRANCH.key(), branch);
            return this;
        }

        public Builder setAsyncFlush(boolean asyncFlush) {
            return set("asyncFlush", Boolean.toString(asyncFlush));
        }

        public Builder setMaxWriteCount(int maxWriteCount) {
            return set("maxWriteCount", Integer.toString(maxWriteCount));
        }

        public Builder setCorePoolSize(int corePoolSize) {
            return set("corePoolSize", Integer.toString(corePoolSize));
        }

        public Builder setMaxContinuousEmptyCommits(int maxContinuousEmptyCommits) {
            return set("flink.max-continuous-empty-commits", Integer.toString(maxContinuousEmptyCommits));
        }

        private <T> DataStreamSink<T> chainIcebergOperators() {
            Preconditions.checkArgument(
                    rowDataInput != null,
                    "Please use forRowData() to initialize the input DataStream.");
            // Distribute the records from input data stream based on the write.distribution-mode and
            // equality fields.
            DataStream<RowDataWithTable> distributeStream = distributeDataStream(rowDataInput);

            // Add parallel writers that append rows to files
            SingleOutputStreamOperator<WriteResultWithTable> writerStream = appendWriter(distributeStream);

            // Add single-parallelism committer that commits files
            // after successful checkpoint or end of input
            SingleOutputStreamOperator<Void> committerStream = appendCommitter(writerStream);

            // Add dummy discard sink
            return appendDummySink(committerStream);
        }

        /**
         * Append the iceberg sink operators to write records to iceberg table.
         *
         * @return {@link DataStreamSink} for sink.
         */
        public DataStreamSink<Void> append() {
            return chainIcebergOperators();
        }

        @SuppressWarnings("unchecked")
        private <T> DataStreamSink<T> appendDummySink(
                SingleOutputStreamOperator<Void> committerStream) {
            return committerStream
                    .addSink(new DiscardingSink())
                    .name("DynamicIcebergSink")
                    .uid("DynamicIceberg-dummysink");
        }

        private SingleOutputStreamOperator<Void> appendCommitter(SingleOutputStreamOperator<WriteResultWithTable> writerStream) {
            DynamicIcebergFilesCommitter filesCommitter = new DynamicIcebergFilesCommitter(writeOptions, snapshotProperties);
            int parallelism = writeParallelism == null ? writerStream.getParallelism() : writeParallelism;
            return writerStream
                    .keyBy(WriteResultWithTable::getTableName)
                    .transform(ICEBERG_FILES_COMMITTER_NAME, Types.VOID, filesCommitter)
                    .setParallelism(parallelism)
                    .uid("iceberg-table-streaming-committer");
        }

        private SingleOutputStreamOperator<WriteResultWithTable> appendWriter(DataStream<RowDataWithTable> input) {
            DynamicIcebergStreamWriter streamWriter = createStreamWriter(this.writeOptions, this.readableConfig);
            int parallelism = writeParallelism == null ? input.getParallelism() : writeParallelism;
            return input.transform(
                    ICEBERG_STREAM_WRITER_NAME,
                    TypeInformation.of(WriteResultWithTable.class),
                    streamWriter)
                    .setParallelism(parallelism)
                    .uid("iceberg-table-streaming-writer");
        }

        private DataStream<RowDataWithTable> distributeDataStream(DataStream<RowDataWithTable> input) {
            String writeMode = writeOptions.getOrDefault(FlinkWriteOptions.DISTRIBUTION_MODE.key(), "NONE");
            LOG.info("Write distribution mode is '{}'", writeMode);
            switch (writeMode) {
                case "NONE":
                    LOG.info("Distribute rows by equality fields, because there are equality fields set");
                    return input.keyBy(new DynamicEqualityFieldKeySelector());
                case "HASH":
                    return input.keyBy(new DynamicPartitionKeySelector());
                case "RANGE":
                    LOG.info("Distribute rows by equality fields, because there are equality fields set "
                                    + "and{}=range is not supported yet in flink",
                            WRITE_DISTRIBUTION_MODE);
                    return input.keyBy(new DynamicEqualityFieldKeySelector());
                default:
                    throw new RuntimeException("Unrecognized " + WRITE_DISTRIBUTION_MODE + ": " + writeMode);
            }
        }
    }

    static RowType toFlinkRowType(Schema schema, TableSchema requestedSchema) {
        if (requestedSchema != null) {
            // Convert the flink schema to iceberg schema firstly, then reassign ids to match the existing
            // iceberg schema.
            Schema writeSchema = TypeUtil.reassignIds(FlinkSchemaUtil.convert(requestedSchema), schema);
            TypeUtil.validateWriteSchema(schema, writeSchema, true, true);
            // We use this flink schema to read values from RowDataWithTable. The flink's TINYINT and SMALLINT will
            // be promoted to
            // iceberg INTEGER, that means if we use iceberg's table schema to read TINYINT (backend by 1
            // 'byte'), we will
            // read 4 bytes rather than 1 byte, it will mess up the byte array in BinaryRowData. So here
            // we must use flink
            // schema.
            return (RowType) requestedSchema.toRowDataType().getLogicalType();
        } else {
            return FlinkSchemaUtil.convert(schema);
        }
    }

    static DynamicIcebergStreamWriter createStreamWriter(Map<String, String> writeOptions, ReadableConfig readableConfig) {
        DynamicTaskWriterFactory<RowData> taskWriterFactory = new DynamicRowDataTaskWriterFactory(writeOptions, readableConfig);
        String async = writeOptions.getOrDefault("asyncFlush", "true");
        String corePoolSize = writeOptions.getOrDefault("corePoolSize",
                FlinkConfigOptions.TABLE_EXEC_ICEBERG_WORKER_POOL_SIZE.defaultValue().toString());
        String maxWriteCount = writeOptions.getOrDefault("maxWriteCount", "500");
        return new DynamicIcebergStreamWriter(
                taskWriterFactory,
                Boolean.parseBoolean(async),
                Integer.parseInt(corePoolSize),
                Integer.parseInt(maxWriteCount));
    }

}

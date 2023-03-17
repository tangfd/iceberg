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
import org.apache.iceberg.io.WriteResult;
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
        private String uidPrefix = null;
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

        /**
         * Set the uid prefix for FlinkSink operators. Note that FlinkSink internally consists of multiple operators
         * (like writer, committer, dummy sink etc.) Actually operator uid will be appended with a suffix like
         * "uidPrefix-writer". <br>
         * <br>
         * If provided, this prefix is also applied to operator names. <br>
         * <br>
         * Flink auto generates operator uid if not set explicitly. It is a recommended <a
         * href="https://ci.apache.org/projects/flink/flink-docs-master/docs/ops/production_ready/"> best-practice to
         * set uid for all operators</a> before deploying to production. Flink has an option to {@code
         * pipeline.auto-generate-uid=false} to disable auto-generation and force explicit setting of all operator uid.
         * <br>
         * <br>
         * Be careful with setting this for an existing job, because now we are changing the operator uid from an
         * auto-generated one to this new value. When deploying the change with a checkpoint, Flink won't be able to
         * restore the previous Flink sink operator state (more specifically the committer operator state). You need to
         * use {@code --allowNonRestoredState} to ignore the previous sink state. During restore Flink sink state is
         * used to check if last commit was actually successful or not. {@code --allowNonRestoredState} can lead to data
         * loss if the Iceberg commit failed in the last completed checkpoint.
         *
         * @param newPrefix prefix for Flink sink operator uid and name
         * @return {@link Builder} to connect the iceberg table.
         */
        public Builder uidPrefix(String newPrefix) {
            this.uidPrefix = newPrefix;
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

        private <T> DataStreamSink<T> chainIcebergOperators() {
            Preconditions.checkArgument(
                    rowDataInput != null,
                    "Please use forRowData() to initialize the input DataStream.");
            // Distribute the records from input data stream based on the write.distribution-mode and
            // equality fields.
            DataStream<RowDataWithTable> distributeStream = distributeDataStream(rowDataInput);

            // Add parallel writers that append rows to files
            SingleOutputStreamOperator<WriteResult> writerStream = appendWriter(distributeStream);

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
                    .setParallelism(1)
                    .uid("DynamicIceberg-dummysink");
        }

        private SingleOutputStreamOperator<Void> appendCommitter(SingleOutputStreamOperator<WriteResult> writerStream) {
            DynamicIcebergFilesCommitter filesCommitter = new DynamicIcebergFilesCommitter(writeOptions, snapshotProperties);
            return writerStream
                    .transform(ICEBERG_FILES_COMMITTER_NAME, Types.VOID, filesCommitter)
                    .setParallelism(1)
                    .setMaxParallelism(1)
                    .uid("iceberg-table-streaming-committer");
        }

        private SingleOutputStreamOperator<WriteResult> appendWriter(DataStream<RowDataWithTable> input) {
            DynamicIcebergStreamWriter streamWriter = createStreamWriter(this.writeOptions, this.readableConfig);
            int parallelism = writeParallelism == null ? input.getParallelism() : writeParallelism;
            return input.transform(
                    ICEBERG_STREAM_WRITER_NAME,
                    TypeInformation.of(WriteResult.class),
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

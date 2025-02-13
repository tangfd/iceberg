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
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.commons.collections.MapUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.sink.IcebergStreamWriterMetrics;
import org.apache.iceberg.flink.sink.dynamic.table.IcebergTableServiceLoader;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.ThreadPools;

class DynamicIcebergStreamWriter extends AbstractStreamOperator<WriteResultWithTable>
        implements OneInputStreamOperator<RowDataWithTable, WriteResultWithTable>, BoundedOneInput {

    private static final long serialVersionUID = 1L;

    private final DynamicTaskWriterFactory<RowData> taskWriterFactory;
    private final ParameterTool param;

    private transient Map<String, TaskWriter<RowData>> writerMap;
    private transient Cache<String, IcebergStreamWriterMetrics> metricsCache;
    private transient int subTaskId;
    private transient int attemptId;
    private transient int currentCount = 0;
    private transient ExecutorService executor;
    private final boolean asyncFlush;
    private final int maxWriteCount;
    private final int corePoolSize;

    DynamicIcebergStreamWriter(DynamicTaskWriterFactory<RowData> taskWriterFactory,
                               boolean asyncFlush, int corePoolSize, int maxWriteCount, ParameterTool param) {
        this.asyncFlush = asyncFlush;
        this.maxWriteCount = maxWriteCount;
        this.corePoolSize = corePoolSize;
        this.taskWriterFactory = taskWriterFactory;
        this.param = param;
        setChainingStrategy(ChainingStrategy.ALWAYS);
    }

    @Override
    public void open() {
        this.subTaskId = getRuntimeContext().getIndexOfThisSubtask();
        this.attemptId = getRuntimeContext().getAttemptNumber();
        this.writerMap = new ConcurrentHashMap<>(256);
        this.metricsCache = CacheUtils.createCache();
        if (asyncFlush) {
            Preconditions.checkArgument(corePoolSize > 0, "corePoolSize must be more than 0");
            executor = ThreadPools.newWorkerPool("flush-write-data-thread", corePoolSize);
        }
    }

    @Override
    public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
        flush();
    }

    @Override
    public void processElement(StreamRecord<RowDataWithTable> element) throws Exception {
        RowDataWithTable rowDataWithTable = element.getValue();
        TaskWriter<RowData> writer = writerMap.computeIfAbsent(rowDataWithTable.getTable(),
                t -> create(rowDataWithTable.tableInfo));
        writer.write(rowDataWithTable.rowData);
        if (++this.currentCount > this.maxWriteCount) {
            flush();
        }
    }

    private TaskWriter<RowData> create(TableInfo tableInfo) {
        return taskWriterFactory.create(tableInfo, this.subTaskId, this.attemptId);
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (MapUtils.isEmpty(writerMap)) {
            return;
        }

        if (executor != null) {
            executor.shutdown();
        }

        for (Iterator<Map.Entry<String, TaskWriter<RowData>>> iterator = writerMap.entrySet().iterator(); iterator.hasNext(); ) {
            Map.Entry<String, TaskWriter<RowData>> entry = iterator.next();
            iterator.remove();
            entry.getValue().close();
        }
    }

    @Override
    public void endInput() throws IOException {
        // For bounded stream, it may don't enable the checkpoint mechanism so we'd better to emit the
        // remaining completed files to downstream before closing the writer so that we won't miss any
        // of them.
        // Note that if the task is not closed after calling endInput, checkpoint may be triggered again
        // causing files to be sent repeatedly, the writer is marked as null after the last file is sent
        // to guard against duplicated writes.
        flush();
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("table_name", writerMap.keySet())
                .add("subtask_id", subTaskId)
                .add("attempt_id", attemptId)
                .toString();
    }

    /**
     * close all open files and emit files to downstream committer operator
     */
    private void flush() throws IOException {
        if (MapUtils.isEmpty(writerMap)) {
            return;
        }

        this.currentCount = 0;
        if (!asyncFlush) {
            for (Iterator<Map.Entry<String, TaskWriter<RowData>>> iterator = writerMap.entrySet().iterator(); iterator.hasNext(); ) {
                Map.Entry<String, TaskWriter<RowData>> entry = iterator.next();
                iterator.remove();
                flush(entry.getValue(), entry.getKey());
            }

            return;
        }

        Preconditions.checkArgument(executor != null, "writer executor shouldn't be null");
        List<Future<Object>> futures = new ArrayList<>(writerMap.size());
        for (Iterator<Map.Entry<String, TaskWriter<RowData>>> iterator = writerMap.entrySet().iterator(); iterator.hasNext(); ) {
            Map.Entry<String, TaskWriter<RowData>> entry = iterator.next();
            iterator.remove();
            futures.add(executor.submit(() -> {
                flush(entry.getValue(), entry.getKey());
                return null;
            }));
        }

        for (Future<Object> future : futures) {
            try {
                future.get();
            } catch (Exception e) {
                LOG.error("flush RowData error!", e);
            }
        }
    }

    /**
     * close all open files and emit files to downstream committer operator
     */
    private void flush(TaskWriter<RowData> writer, String tableName) throws IOException {
        if (writer == null) {
            return;
        }

        long startNano = System.nanoTime();
        WriteResult result = writer.complete();
        Table table = IcebergTableServiceLoader.loadExistTableWithCache(tableName, param);
        IcebergStreamWriterMetrics writerMetrics = metricsCache.get(tableName,
                t -> new IcebergStreamWriterMetrics(super.metrics, table.name()));
        writerMetrics.updateFlushResult(result);
        output.collect(new StreamRecord<>(new WriteResultWithTable(result, tableName)));
        writerMetrics.flushDuration(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNano));
    }
}

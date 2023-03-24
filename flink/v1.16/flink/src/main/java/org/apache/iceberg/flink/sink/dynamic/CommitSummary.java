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

import java.util.Arrays;
import java.util.NavigableMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;

public class CommitSummary {

    private final AtomicLong dataFilesCount = new AtomicLong();
    private final AtomicLong dataFilesRecordCount = new AtomicLong();
    private final AtomicLong dataFilesByteCount = new AtomicLong();
    private final AtomicLong deleteFilesCount = new AtomicLong();
    private final AtomicLong deleteFilesRecordCount = new AtomicLong();
    private final AtomicLong deleteFilesByteCount = new AtomicLong();

    public CommitSummary(NavigableMap<Long, WriteResult> pendingResults) {
        pendingResults
                .values()
                .forEach(this::init);
    }

    public CommitSummary(WriteResult writeResult) {
        init(writeResult);
    }

    private void init(WriteResult writeResult) {
        dataFilesCount.addAndGet(writeResult.dataFiles().length);
        Arrays.stream(writeResult.dataFiles())
                .forEach(dataFile -> {
                    dataFilesRecordCount.addAndGet(dataFile.recordCount());
                    dataFilesByteCount.addAndGet(dataFile.fileSizeInBytes());
                });
        deleteFilesCount.addAndGet(writeResult.deleteFiles().length);
        Arrays.stream(writeResult.deleteFiles())
                .forEach(deleteFile -> {
                    deleteFilesRecordCount.addAndGet(deleteFile.recordCount());
                    deleteFilesByteCount.addAndGet(deleteFile.fileSizeInBytes());
                });
    }

    public long dataFilesCount() {
        return dataFilesCount.get();
    }

    long dataFilesRecordCount() {
        return dataFilesRecordCount.get();
    }

    long dataFilesByteCount() {
        return dataFilesByteCount.get();
    }

    public long deleteFilesCount() {
        return deleteFilesCount.get();
    }

    long deleteFilesRecordCount() {
        return deleteFilesRecordCount.get();
    }

    long deleteFilesByteCount() {
        return deleteFilesByteCount.get();
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("dataFilesCount", dataFilesCount)
                .add("dataFilesRecordCount", dataFilesRecordCount)
                .add("dataFilesByteCount", dataFilesByteCount)
                .add("deleteFilesCount", deleteFilesCount)
                .add("deleteFilesRecordCount", deleteFilesRecordCount)
                .add("deleteFilesByteCount", deleteFilesByteCount)
                .toString();
    }

    public void add(CommitSummary summary) {
        this.dataFilesCount.addAndGet(summary.dataFilesCount());
        this.dataFilesRecordCount.addAndGet(summary.dataFilesRecordCount());
        this.dataFilesByteCount.addAndGet(summary.dataFilesByteCount());
        this.deleteFilesCount.addAndGet(summary.deleteFilesCount());
        this.deleteFilesRecordCount.addAndGet(summary.deleteFilesRecordCount());
        this.deleteFilesByteCount.addAndGet(summary.deleteFilesByteCount());
    }
}

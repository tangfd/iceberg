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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.runtime.typeutils.SortedMapTypeInfo;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ReplacePartitions;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotUpdate;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.FlinkConfigOptions;
import org.apache.iceberg.flink.FlinkWriteOptions;
import org.apache.iceberg.flink.sink.DeltaManifests;
import org.apache.iceberg.flink.sink.DeltaManifestsSerializer;
import org.apache.iceberg.flink.sink.FlinkManifestUtil;
import org.apache.iceberg.flink.sink.ManifestOutputFileFactory;
import org.apache.iceberg.flink.sink.dynamic.table.IcebergTableServiceLoader;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Strings;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.ThreadPools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class DynamicIcebergFilesCommitter extends AbstractStreamOperator<Void>
        implements OneInputStreamOperator<WriteResultWithTable, Void>, BoundedOneInput {

    private static final long serialVersionUID = 1L;
    private static final long INITIAL_CHECKPOINT_ID = -1L;
    private static final Map<String, byte[]> EMPTY_MANIFEST_DATA = new HashMap<>(0);

    private static final Logger LOG = LoggerFactory.getLogger(DynamicIcebergFilesCommitter.class);
    private static final String FLINK_JOB_ID = "flink.job-id";
    private static final String OPERATOR_ID = "flink.operator-id";

    // The max checkpoint id we've committed to iceberg table. As the flink's checkpoint is always
    // increasing, so we could correctly commit all the data files whose checkpoint id is greater than
    // the max committed one to iceberg table, for avoiding committing the same data files twice. This
    // id will be attached to iceberg's meta when committing the iceberg transaction.
    private static final String MAX_COMMITTED_CHECKPOINT_ID = "flink.max-committed-checkpoint-id";
    static final String MAX_CONTINUOUS_EMPTY_COMMITS = "flink.max-continuous-empty-commits";

    private final boolean replacePartitions;
    private final Map<String, String> snapshotProperties;
    private final Map<String, String> writeOptions;
    private final ParameterTool param;

    // A sorted map to maintain the completed data files for each pending checkpointId (which have not
    // been committed to iceberg table). We need a sorted map here because there's possible that few
    // checkpoints snapshot failed, for example: the 1st checkpoint have 2 data files <1, <file0,
    // file1>>, the 2st checkpoint have 1 data files <2, <file3>>. Snapshot for checkpoint#1
    // interrupted because of network/disk failure etc, while we don't expect any data loss in iceberg
    // table. So we keep the finished files <1, <file0, file1>> in memory and retry to commit iceberg
    // table when the next checkpoint happen.
    private final NavigableMap<Long, Map<String, byte[]>> dataFilesPerCheckpoint = Maps.newTreeMap();

    // The completed files cache for current checkpoint. Once the snapshot barrier received, it will
    // be flushed to the 'dataFilesPerCheckpoint'.
    private final Map<String, List<WriteResult>> writeResultsOfCurrentCkpt = new ConcurrentHashMap<>(32);
    private final String branch;

    // It will have an unique identifier for one job.
    private transient String flinkJobId;
    private transient String operatorUniqueId;
    private final transient Map<String, IcebergFilesCommitterMetrics> committerMetricsMap = new HashMap<>(32);
    private final transient Map<String, ManifestOutputFileFactory> manifestOutputFileFactoryMap = new HashMap<>(32);
    private transient long maxCommittedCheckpointId;
    private transient int continuousEmptyCheckpoints;
    private transient int maxContinuousEmptyCommits;
    private transient int subTaskId;
    private transient int attemptId;
    // There're two cases that we restore from flink checkpoints: the first case is restoring from
    // snapshot created by the same flink job; another case is restoring from snapshot created by
    // another different job. For the second case, we need to maintain the old flink job's id in flink
    // state backend to find the max-committed-checkpoint-id when traversing iceberg table's
    // snapshots.
    private static final ListStateDescriptor<String> JOB_ID_DESCRIPTOR =
            new ListStateDescriptor<>("iceberg-flink-job-id", BasicTypeInfo.STRING_TYPE_INFO);
    private transient ListState<String> jobIdState;
    // All pending checkpoints states for this function.
    private static final ListStateDescriptor<SortedMap<Long, Map<String, byte[]>>> STATE_DESCRIPTOR = buildStateDescriptor();
    private transient ListState<SortedMap<Long, Map<String, byte[]>>> checkpointsState;

    private final Integer workerPoolSize;
    private transient ExecutorService workerPool;

    DynamicIcebergFilesCommitter(Map<String, String> writeOptions, Map<String, String> snapshotProperties, ParameterTool param) {
        this.replacePartitions = Boolean.parseBoolean(writeOptions.get(FlinkWriteOptions.OVERWRITE_MODE.key()));
        this.snapshotProperties = snapshotProperties;
        this.writeOptions = writeOptions;
        this.param = param;
        this.workerPoolSize = FlinkConfigOptions.TABLE_EXEC_ICEBERG_WORKER_POOL_SIZE.defaultValue();
        this.branch = StringUtils.defaultString(writeOptions.get(FlinkWriteOptions.BRANCH.key()),
                FlinkWriteOptions.BRANCH.defaultValue());
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);
        this.flinkJobId = getContainingTask().getEnvironment().getJobID().toString();
        this.operatorUniqueId = getRuntimeContext().getOperatorUniqueID();

        maxContinuousEmptyCommits =
                PropertyUtil.propertyAsInt(this.writeOptions, MAX_CONTINUOUS_EMPTY_COMMITS, 10);
        Preconditions.checkArgument(
                maxContinuousEmptyCommits > 0, MAX_CONTINUOUS_EMPTY_COMMITS + " must be positive");
        this.subTaskId = getRuntimeContext().getIndexOfThisSubtask();
        this.attemptId = getRuntimeContext().getAttemptNumber();
        this.maxCommittedCheckpointId = INITIAL_CHECKPOINT_ID;

        this.checkpointsState = context.getOperatorStateStore().getListState(STATE_DESCRIPTOR);
        this.jobIdState = context.getOperatorStateStore().getListState(JOB_ID_DESCRIPTOR);
        if (!context.isRestored()) {
            return;
        }

        Iterable<String> jobIdIterable = jobIdState.get();
        if (jobIdIterable == null || !jobIdIterable.iterator().hasNext()) {
            LOG.warn("Failed to restore committer state. This can happen when operator uid changed and Flink "
                    + "allowNonRestoredState is enabled. Best practice is to explicitly set the operator id "
                    + "via FlinkSink#Builder#uidPrefix() so that the committer operator uid is stable. "
                    + "Otherwise, Flink auto generate an operator uid based on job topology."
                    + "With that, operator uid is subjective to change upon topology change.");
            return;
        }

        String restoredFlinkJobId = jobIdIterable.iterator().next();
        Preconditions.checkState(!Strings.isNullOrEmpty(restoredFlinkJobId),
                "Flink job id parsed from checkpoint snapshot shouldn't be null or empty");
        // Since flink's checkpoint id will start from the max-committed-checkpoint-id + 1 in the new
        // flink job even if it's restored from a snapshot created by another different flink job, so
        // it's safe to assign the max committed checkpoint id from restored flink job to the current
        // flink job.
        NavigableMap<Long, Map<String, byte[]>> state = Maps.newTreeMap(checkpointsState.get().iterator().next());
        if (MapUtils.isEmpty(state)) {
            return;
        }

        Map<String, NavigableMap<Long, Map<String, byte[]>>> dataFiles = groupByTable(state);
        for (Map.Entry<String, NavigableMap<Long, Map<String, byte[]>>> entry : dataFiles.entrySet()) {
            Table table = IcebergTableServiceLoader.loadExistTableWithCache(entry.getKey(), param);
            long tableMaxCommittedCheckpointId = getMaxCommittedCheckpointId(table, restoredFlinkJobId, operatorUniqueId, branch);
            NavigableMap<Long, Map<String, byte[]>> uncommittedDataFiles = entry.getValue().tailMap(tableMaxCommittedCheckpointId, false);
            if (MapUtils.isNotEmpty(uncommittedDataFiles)) {
                // Committed all uncommitted data files from the old flink job to iceberg table.
                long maxUncommittedCheckpointId = uncommittedDataFiles.lastKey();
                commitUpToCheckpoint(uncommittedDataFiles, restoredFlinkJobId, operatorUniqueId, maxUncommittedCheckpointId);
            }

            if (this.maxCommittedCheckpointId < tableMaxCommittedCheckpointId) {
                this.maxCommittedCheckpointId = tableMaxCommittedCheckpointId;
            }
        }
    }

    private Map<String, NavigableMap<Long, Map<String, byte[]>>> groupByTable(NavigableMap<Long, Map<String, byte[]>> state) {
        Map<String, NavigableMap<Long, Map<String, byte[]>>> tableState = new HashMap<>();
        for (Map.Entry<Long, Map<String, byte[]>> entry : state.entrySet()) {
            for (Map.Entry<String, byte[]> map : entry.getValue().entrySet()) {
                NavigableMap<Long, Map<String, byte[]>> navigableMap = tableState.computeIfAbsent(map.getKey(), t -> Maps.newTreeMap());
                navigableMap.computeIfAbsent(entry.getKey(), t -> new HashMap<>()).put(map.getKey(), map.getValue());
            }
        }

        return tableState;
    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);
        long checkpointId = context.getCheckpointId();
        // Update the checkpoint state.
        long startNano = System.nanoTime();
        Map<String, byte[]> manifests = writeToManifest(checkpointId);
        LOG.info("Start to flush snapshot state to state backend, table: {}, checkpointId: {}", manifests.keySet(), checkpointId);
        dataFilesPerCheckpoint.put(checkpointId, manifests);
        // Reset the snapshot state to the latest state.
        checkpointsState.clear();
        checkpointsState.add(dataFilesPerCheckpoint);
        jobIdState.clear();
        jobIdState.add(flinkJobId);
        // Clear the local buffer for current checkpoint.
        writeResultsOfCurrentCkpt.clear();
        for (String tableName : manifests.keySet()) {
            Table table = IcebergTableServiceLoader.loadExistTableWithCache(tableName, param);
            IcebergFilesCommitterMetrics committerMetrics = committerMetricsMap.get(table.name());
            if (committerMetrics != null) {
                committerMetrics.checkpointDuration(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNano));
            }
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        super.notifyCheckpointComplete(checkpointId);
        // It's possible that we have the following events:
        //   1. snapshotState(ckpId);
        //   2. snapshotState(ckpId+1);
        //   3. notifyCheckpointComplete(ckpId+1);
        //   4. notifyCheckpointComplete(ckpId);
        // For step#4, we don't need to commit iceberg table again because in step#3 we've committed all
        // the files,
        // Besides, we need to maintain the max-committed-checkpoint-id to be increasing.
        if (checkpointId > maxCommittedCheckpointId) {
            LOG.info("Checkpoint {} completed. Attempting commit.", checkpointId);
            commitUpToCheckpoint(dataFilesPerCheckpoint, flinkJobId, operatorUniqueId, checkpointId);
            this.maxCommittedCheckpointId = checkpointId;
        } else {
            LOG.info("Skipping committing checkpoint {}. {} is already committed.", checkpointId, maxCommittedCheckpointId);
        }
    }

    private void commitUpToCheckpoint(
            NavigableMap<Long, Map<String, byte[]>> deltaManifestsMap,
            String newFlinkJobId,
            String operatorId,
            long checkpointId)
            throws IOException {
        NavigableMap<Long, Map<String, byte[]>> pendingMap = deltaManifestsMap.headMap(checkpointId, true);
        Map<Table, List<ManifestFile>> manifestMap = new HashMap<>(32);
        Map<Table, NavigableMap<Long, WriteResultWithSummary>> pendingResults = new HashMap<>(32);
        Map<Table, Tuple2<Long, Long>> filesCountMap = new HashMap<>(32);
        for (Map.Entry<Long, Map<String, byte[]>> e : pendingMap.entrySet()) {
            Map<String, byte[]> values = e.getValue();
            if (MapUtils.isEmpty(values)) {
                // Skip the empty flink manifest.
                continue;
            }

            for (Map.Entry<String, byte[]> entry : values.entrySet()) {
                Table table = IcebergTableServiceLoader.loadExistTableWithCache(entry.getKey(), param);
                DeltaManifests deltaManifests =
                        SimpleVersionedSerialization.readVersionAndDeSerialize(DeltaManifestsSerializer.INSTANCE, entry.getValue());
                List<ManifestFile> manifests = manifestMap.computeIfAbsent(table, t -> new ArrayList<>());
                manifests.addAll(deltaManifests.manifests());
                NavigableMap<Long, WriteResultWithSummary> result = pendingResults.computeIfAbsent(table, t -> Maps.newTreeMap());
                WriteResult writeResult = FlinkManifestUtil.readCompletedFiles(deltaManifests, table.io());
                CommitSummary summary = new CommitSummary(writeResult);
                Tuple2<Long, Long> filesCount = filesCountMap.getOrDefault(table, Tuple2.of(0L, 0L));
                long total = filesCount.f0 + summary.dataFilesCount() + summary.deleteFilesCount();
                long delete = filesCount.f1 + summary.deleteFilesCount();
                filesCountMap.put(table, Tuple2.of(total, delete));
                result.put(e.getKey(), new WriteResultWithSummary(writeResult, summary));
            }
        }

        commitPendingResult(pendingResults, filesCountMap, newFlinkJobId, operatorId, checkpointId);
        for (Map.Entry<Table, NavigableMap<Long, WriteResultWithSummary>> entry : pendingResults.entrySet()) {
            IcebergFilesCommitterMetrics committerMetrics = committerMetricsMap.get(entry.getKey().name());
            entry.getValue().values().stream()
                    .map(WriteResultWithSummary::getSummary)
                    .forEach(committerMetrics::updateCommitSummary);
        }

        pendingMap.clear();
        deleteCommittedManifests(manifestMap, newFlinkJobId, checkpointId);
    }

    private void commitPendingResult(
            Map<Table, NavigableMap<Long, WriteResultWithSummary>> pendingResults,
            Map<Table, Tuple2<Long, Long>> filesCountMap,
            String newFlinkJobId,
            String operatorId,
            long checkpointId) {
        for (Map.Entry<Table, NavigableMap<Long, WriteResultWithSummary>> entry : pendingResults.entrySet()) {
            Table table = entry.getKey();
            Tuple2<Long, Long> filesCount = filesCountMap.getOrDefault(table, Tuple2.of(0L, 0L));
            continuousEmptyCheckpoints = filesCount.f0 == 0 ? continuousEmptyCheckpoints + 1 : 0;
            if (filesCount.f0 != 0 || continuousEmptyCheckpoints % maxContinuousEmptyCommits == 0) {
                NavigableMap<Long, WriteResultWithSummary> result = entry.getValue();
                if (replacePartitions) {
                    replacePartitions(result, table, filesCount.f1, newFlinkJobId, operatorId, checkpointId);
                } else {
                    commitDeltaTxn(result, table, filesCount.f1, newFlinkJobId, operatorId, checkpointId);
                }
                continuousEmptyCheckpoints = 0;
            } else {
                LOG.info("Skip commit for checkpoint {} due to no data files or delete files.", checkpointId);
            }
        }
    }

    private void deleteCommittedManifests(Map<Table, List<ManifestFile>> manifestMap,
                                          String newFlinkJobId,
                                          long checkpointId) {
        for (Map.Entry<Table, List<ManifestFile>> entry : manifestMap.entrySet()) {
            FileIO fileIO = entry.getKey().io();
            for (ManifestFile manifest : entry.getValue()) {
                try {
                    fileIO.deleteFile(manifest.path());
                } catch (Exception e) {
                    // The flink manifests cleaning failure shouldn't abort the completed checkpoint.
                    String details = MoreObjects.toStringHelper(this)
                            .add("flinkJobId", newFlinkJobId)
                            .add("checkpointId", checkpointId)
                            .add("manifestPath", manifest.path())
                            .toString();
                    LOG.warn("The iceberg transaction has been committed, but we failed to clean the temporary flink manifests: {}",
                            details, e);
                }
            }
        }
    }

    private void replacePartitions(
            NavigableMap<Long, WriteResultWithSummary> pendingResults,
            Table table,
            long deleteFilesCount,
            String newFlinkJobId,
            String operatorId,
            long checkpointId) {
        Preconditions.checkState(deleteFilesCount == 0, "Cannot overwrite partitions with delete files.");
        // Commit the overwrite transaction.
        CommitSummary totalSummary = null;
        ReplacePartitions dynamicOverwrite = table.newReplacePartitions().scanManifestsWith(workerPool);
        for (WriteResultWithSummary result : pendingResults.values()) {
            WriteResult writeResult = result.getWriteResult();
            Preconditions.checkState(writeResult.referencedDataFiles().length == 0,
                    "Should have no referenced data files.");
            Arrays.stream(writeResult.dataFiles()).forEach(dynamicOverwrite::addFile);
            CommitSummary summary = result.getSummary();
            if (totalSummary == null) {
                totalSummary = summary;
            } else {
                totalSummary.add(summary);
            }
        }

        commitOperation(dynamicOverwrite, totalSummary, table.name(),
                "dynamic partition overwrite", newFlinkJobId, operatorId, checkpointId);
    }

    private void commitDeltaTxn(
            NavigableMap<Long, WriteResultWithSummary> pendingResults,
            Table table,
            long deleteFilesCount,
            String newFlinkJobId,
            String operatorId,
            long checkpointId) {
        if (deleteFilesCount == 0) {
            // To be compatible with iceberg format V1.
            CommitSummary totalSummary = null;
            AppendFiles appendFiles = table.newAppend().scanManifestsWith(workerPool);
            for (WriteResultWithSummary result : pendingResults.values()) {
                WriteResult writeResult = result.getWriteResult();
                Preconditions.checkState(writeResult.referencedDataFiles().length == 0,
                        "Should have no referenced data files for append.");
                Arrays.stream(writeResult.dataFiles()).forEach(appendFiles::appendFile);
                CommitSummary summary = result.getSummary();
                if (totalSummary == null) {
                    totalSummary = summary;
                } else {
                    totalSummary.add(summary);
                }
            }

            commitOperation(appendFiles, totalSummary, table.name(), "append", newFlinkJobId, operatorId, checkpointId);
            return;
        }

        // To be compatible with iceberg format V2.
        for (Map.Entry<Long, WriteResultWithSummary> e : pendingResults.entrySet()) {
            // We don't commit the merged result into a single transaction because for the sequential
            // transaction txn1 and txn2, the equality-delete files of txn2 are required to be applied
            // to data files from txn1. Committing the merged one will lead to the incorrect delete
            // semantic.
            WriteResultWithSummary result = e.getValue();

            // Row delta validations are not needed for streaming changes that write equality deletes.
            // Equality deletes are applied to data in all previous sequence numbers, so retries may
            // push deletes further in the future, but do not affect correctness. Position deletes
            // committed to the table in this path are used only to delete rows from data files that are
            // being added in this commit. There is no way for data files added along with the delete
            // files to be concurrently removed, so there is no need to validate the files referenced by
            // the position delete files that are being committed.
            RowDelta rowDelta = table.newRowDelta().scanManifestsWith(workerPool);
            WriteResult writeResult = result.getWriteResult();
            Arrays.stream(writeResult.dataFiles()).forEach(rowDelta::addRows);
            Arrays.stream(writeResult.deleteFiles()).forEach(rowDelta::addDeletes);
            commitOperation(rowDelta, result.getSummary(), table.name(), "rowDelta", newFlinkJobId, operatorId, e.getKey());
        }
    }

    private void commitOperation(
            SnapshotUpdate<?> operation,
            CommitSummary summary,
            String table,
            String description,
            String newFlinkJobId,
            String operatorId,
            long checkpointId) {
        LOG.info("Committing {} for checkpoint {} to table {} branch {} with summary: {}",
                description, checkpointId, table, branch, summary);
        snapshotProperties.forEach(operation::set);
        // custom snapshot metadata properties will be overridden if they conflict with internal ones
        // used by the sink.
        operation.set(MAX_COMMITTED_CHECKPOINT_ID, Long.toString(checkpointId));
        operation.set(FLINK_JOB_ID, newFlinkJobId);
        operation.set(OPERATOR_ID, operatorId);
        operation.toBranch(branch);
        long startNano = System.nanoTime();
        operation.commit(); // abort is automatically called if this fails.
        long durationMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNano);
        LOG.info("Committed {} to table: {}, branch: {}, checkpointId {} in {} ms",
                description, table, branch, checkpointId, durationMs);
        IcebergFilesCommitterMetrics committerMetrics = committerMetricsMap.computeIfAbsent(table,
                t -> new IcebergFilesCommitterMetrics(super.metrics, table));
        committerMetrics.commitDuration(durationMs);
    }

    @Override
    public void processElement(StreamRecord<WriteResultWithTable> element) {
        WriteResultWithTable result = element.getValue();
        this.writeResultsOfCurrentCkpt.computeIfAbsent(result.getTableName(), t -> new ArrayList<>())
                .add(result.getWriteResult());
    }

    @Override
    public void endInput() throws IOException {
        // Flush the buffered data files into 'dataFilesPerCheckpoint' firstly.
        long currentCheckpointId = Long.MAX_VALUE;
        dataFilesPerCheckpoint.put(currentCheckpointId, writeToManifest(currentCheckpointId));
        writeResultsOfCurrentCkpt.clear();
        commitUpToCheckpoint(dataFilesPerCheckpoint, flinkJobId, operatorUniqueId, currentCheckpointId);
    }

    /**
     * Write all the complete data files to a newly created manifest file and return the manifest's avro serialized
     * bytes.
     */
    private Map<String, byte[]> writeToManifest(long checkpointId) throws IOException {
        if (writeResultsOfCurrentCkpt.isEmpty()) {
            return EMPTY_MANIFEST_DATA;
        }

        Map<String, byte[]> tableManifests = new HashMap<>(writeResultsOfCurrentCkpt.size());
        for (Map.Entry<String, List<WriteResult>> entry : writeResultsOfCurrentCkpt.entrySet()) {
            WriteResult result = WriteResult.builder().addAll(entry.getValue()).build();
            String tableName = entry.getKey();
            Table table = IcebergTableServiceLoader.loadExistTableWithCache(tableName, param);
            ManifestOutputFileFactory manifestOutputFileFactory = manifestOutputFileFactoryMap.computeIfAbsent(tableName,
                    t -> FlinkManifestUtil.createOutputFileFactory(table, flinkJobId, operatorUniqueId, subTaskId, attemptId));
            DeltaManifests deltaManifests =
                    FlinkManifestUtil.writeCompletedFiles(result, () -> manifestOutputFileFactory.create(checkpointId), table.spec());
            byte[] bytes = SimpleVersionedSerialization.writeVersionAndSerialize(DeltaManifestsSerializer.INSTANCE, deltaManifests);
            tableManifests.put(tableName, bytes);
        }

        return tableManifests;
    }

    @Override
    public void open() throws Exception {
        super.open();

        final String operatorID = getRuntimeContext().getOperatorUniqueID();
        this.workerPool = ThreadPools.newWorkerPool("iceberg-worker-pool-" + operatorID, workerPoolSize);
    }

    @Override
    public void close() throws Exception {
        if (workerPool != null) {
            workerPool.shutdown();
        }
    }

    private static ListStateDescriptor<SortedMap<Long, Map<String, byte[]>>> buildStateDescriptor() {
        Comparator<Long> longComparator = Comparators.forType(Types.LongType.get());
        // Construct a SortedMapTypeInfo.
        SortedMapTypeInfo<Long, Map<String, byte[]>> sortedMapTypeInfo =
                new SortedMapTypeInfo<>(
                        BasicTypeInfo.LONG_TYPE_INFO,
                        new MapTypeInfo<>(BasicTypeInfo.STRING_TYPE_INFO, PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO),
                        longComparator);
        return new ListStateDescriptor<>("iceberg-files-committer-state", sortedMapTypeInfo);
    }

    static long getMaxCommittedCheckpointId(Table table, String flinkJobId, String operatorId, String branch) {
        Snapshot snapshot = table.snapshot(branch);
        long lastCommittedCheckpointId = INITIAL_CHECKPOINT_ID;

        while (snapshot != null) {
            Map<String, String> summary = snapshot.summary();
            String snapshotFlinkJobId = summary.get(FLINK_JOB_ID);
            String snapshotOperatorId = summary.get(OPERATOR_ID);
            if (flinkJobId.equals(snapshotFlinkJobId)
                    && (snapshotOperatorId == null || snapshotOperatorId.equals(operatorId))) {
                String value = summary.get(MAX_COMMITTED_CHECKPOINT_ID);
                if (value != null) {
                    lastCommittedCheckpointId = Long.parseLong(value);
                    break;
                }
            }
            Long parentSnapshotId = snapshot.parentId();
            snapshot = parentSnapshotId != null ? table.snapshot(parentSnapshotId) : null;
        }

        return lastCommittedCheckpointId;
    }
}

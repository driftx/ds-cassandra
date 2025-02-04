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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.db.compaction;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.TimeUUID;

/**
 * This is a base abstract implementing some default methods of {@link TableOperation}.
 * <p/>
 * In previous versions it used to be called CompactionInfo and CompactionInfo.Holder.
 * <p/>
 * This class implements serializable to allow structured info to be returned via JMX.
 **/
public abstract class AbstractTableOperation implements TableOperation
{
    private volatile boolean stopRequested = false;
    private volatile StopTrigger trigger = StopTrigger.NONE;

    /**
     * Interrupt the current operation if possible and if the predicate is true.
     *
     * @param trigger cause of compaction interruption
     */
    @Override
    public void stop(StopTrigger trigger)
    {
        this.stopRequested = true;
        if (!this.trigger.isFinal())
            this.trigger = trigger;
    }

    /**
     * @return true if the operation has received a request to be interrupted.
     */
    @Override
    public boolean isStopRequested()
    {
        return stopRequested || (isGlobal() && CompactionManager.instance.isGlobalCompactionPaused());
    }

    /**
     * Return true if the predicate for the given sstables holds, or if the operation
     * does not consider any sstables, in which case it will always return true (the
     * default behaviour).
     */
    @Override
    public boolean shouldStop(Predicate<SSTableReader> predicate)
    {
        OperationProgress progress = getProgress();
        if (progress.sstables.isEmpty())
        {
            return true;
        }
        return progress.sstables.stream().anyMatch(predicate);
    }

    /**
     * @return cause of compaction interruption.
     */
    @Override
    public StopTrigger trigger()
    {
        return trigger;
    }

    /**
     * The progress information for an operation, refer to the description of the class properties.
     */
    public static class OperationProgress implements Serializable, Progress
    {
        private static final long serialVersionUID = 3695381572726744816L;

        /**
         * The table metadata
         */
        private final TableMetadata metadata;
        /**
         * The type of operation
         */
        private final OperationType operationType;
        /**
         * Normally the bytes processed so far by this operation, but depending on the unit it could mean something else, e.g. ranges or keys.
         */
        private final long completed;
        /**
         * The total bytes that need to be processed, for example the size of the input files. Depending on the unit it could mean something else, e.g. ranges or keys.
         */
        private final long total;
        /**
         * The unit for {@link this#completed} and for {@link this#total}.
         */
        private final Unit unit;
        /**
         * The total bytes that have been scanned. For single file operation, it's the same as "completed"
         */
        private final long totalBytesScanned;
        /**
         * A unique ID for this operation
         */
        private final TimeUUID operationId;
        /**
         * A set of SSTables participating in this operation
         */
        private final ImmutableSet<SSTableReader> sstables;
        private final String targetDirectory;

        public OperationProgress(TableMetadata metadata, OperationType operationType, long bytesComplete, long totalBytes, TimeUUID operationId, Collection<SSTableReader> sstables, String targetDirectory)
        {
            this(metadata, operationType, bytesComplete, totalBytes, Unit.BYTES, bytesComplete, operationId, sstables, targetDirectory);
        }

        public OperationProgress(TableMetadata metadata, OperationType operationType, long bytesComplete, long totalBytes, TimeUUID operationId, Collection<? extends SSTableReader> sstables)
        {
            this(metadata, operationType, bytesComplete, totalBytes, Unit.BYTES, bytesComplete, operationId, sstables, null);
        }

        public OperationProgress(TableMetadata metadata, OperationType operationType, long bytesComplete, long totalBytes, long totalBytesScanned, TimeUUID operationId, Collection<? extends SSTableReader> sstables)
        {
            this(metadata, operationType, bytesComplete, totalBytes, Unit.BYTES, totalBytesScanned, operationId, sstables, null);
        }

        public OperationProgress(TableMetadata metadata, OperationType operationType, long completed, long total, Unit unit, TimeUUID operationId, Collection<? extends SSTableReader> sstables, String targetDirectory)
        {
            this(metadata, operationType, completed, total, unit, completed, operationId, sstables, targetDirectory);
        }
        
        public OperationProgress(TableMetadata metadata, OperationType operationType, long completed, long total, Unit unit, long totalBytesScanned, TimeUUID operationId, Collection<? extends SSTableReader> sstables, String targetDirectory)
        {
            this.operationType = operationType;
            this.completed = completed;
            this.total = total;
            this.metadata = metadata;
            this.unit = unit;
            this.totalBytesScanned = totalBytesScanned;
            this.operationId = operationId;
            this.sstables = ImmutableSet.copyOf(sstables);
            this.targetDirectory = targetDirectory;
        }

        /**
         * @return A copy of this OperationProgress with updated progress.
         */
        public OperationProgress forProgress(long complete, long total)
        {
            return new OperationProgress(metadata, operationType, complete, total, unit, complete, operationId, sstables, targetDirectory);
        }

        /**
         * Special operation progress where we always need to cancel the compaction - for example ViewBuilderTask where we don't know
         * the sstables at construction
         */
        public static OperationProgress withoutSSTables(TableMetadata metadata, OperationType tasktype, long completed, long total, AbstractTableOperation.Unit unit, TimeUUID compactionId)
        {
            return withoutSSTables(metadata, tasktype, completed, total, unit, compactionId, null);
        }

        /**
         * Special operation progress where we always need to cancel the compaction - for example AutoSavingCache where we don't know
         * the sstables at construction
         */
        public static OperationProgress withoutSSTables(TableMetadata metadata, OperationType tasktype, long completed, long total, AbstractTableOperation.Unit unit, TimeUUID compactionId, String targetDirectory)
        {
            return new OperationProgress(metadata, tasktype, completed, total, unit, compactionId, ImmutableSet.of(), targetDirectory);
        }

        @Override
        public Optional<String> keyspace()
        {
            return metadata != null ? Optional.of(metadata.keyspace) : Optional.empty();
        }

        @Override
        public Optional<String> table()
        {
            return metadata != null ? Optional.of(metadata.name) : Optional.empty();
        }

        @Override
        public TableMetadata metadata()
        {
            return metadata;
        }

        @Override
        public long completed()
        {
            return completed;
        }

        @Override
        public long total()
        {
            return total;
        }

        @Override
        public OperationType operationType()
        {
            return operationType;
        }

        @Override
        public TimeUUID operationId()
        {
            return operationId;
        }

        @Override
        public Unit unit()
        {
            return unit;
        }

        @Override
        public Set<SSTableReader> sstables()
        {
            return sstables;
        }

        /**
         * @return the total number of units that has been scanned by the operation
         */
        public long totalByteScanned()
        {
            return totalBytesScanned;
        }

        /**
         * Get the directories this compaction could possibly write to.
         *
         * @return the directories that we might write to, or empty list if we don't know the metadata
         * (like for index summary redistribution), or null if we don't have any disk boundaries
         */
        public List<File> getTargetDirectories()
        {
            if (metadata != null && !metadata.isIndex())
            {
                ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(metadata.id);
                if (cfs != null)
                    return cfs.getDirectoriesForFiles(sstables);
            }
            return Collections.emptyList();
        }

        public String targetDirectory()
        {
            if (targetDirectory == null)
                return "";

            try
            {
                return new File(targetDirectory).canonicalPath();
            }
            catch (Throwable t)
            {
                throw new RuntimeException("Unable to resolve canonical path for " + targetDirectory);
            }
        }

        /**
         * Note that this estimate is based on the amount of data we have left to read - it assumes input
         * size == output size for a compaction, which is not really true, but should most often provide a worst case
         * remaining write size.
         */
        public long estimatedRemainingWriteBytes()
        {
            if (unit == Unit.BYTES && operationType.writesData)
                return total() - completed();
            return 0;
        }

        public String toString()
        {
            StringBuilder buff = new StringBuilder();
            buff.append(String.format("%s(%s, %s / %s %s)", operationType, operationId, completed, total, unit));
            if (metadata != null)
            {
                buff.append(String.format("@%s(%s, %s)", metadata.id, metadata.keyspace, metadata.name));
            }
            return buff.toString();
        }

        public Map<String, String> asMap()
        {
            Map<String, String> ret = new HashMap<>(8);
            ret.put(ID, metadata != null ? metadata.id.toString() : "");
            ret.put(KEYSPACE, keyspace().orElse(null));
            ret.put(COLUMNFAMILY, table().orElse(null));
            ret.put(COMPLETED, Long.toString(completed));
            ret.put(TOTAL, Long.toString(total));
            ret.put(OPERATION_TYPE, operationType.toString());
            ret.put(UNIT, unit.toString());
            ret.put(OPERATION_ID, operationId == null ? "" : operationId.toString());
            ret.put(SSTABLES, Joiner.on(',').join(sstables));
            ret.put(TARGET_DIRECTORY, targetDirectory());
            return ret;
        }
    }
}
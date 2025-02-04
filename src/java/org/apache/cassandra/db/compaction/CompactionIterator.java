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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.LongPredicate;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Ordering;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.AbstractCompactionController;
import org.apache.cassandra.db.Columns;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.EmptyIterators;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.partitions.PurgeFunction;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.db.rows.RangeTombstoneBoundMarker;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.cassandra.db.rows.WrappingUnfilteredRowIterator;
import org.apache.cassandra.db.transform.DuplicateRowChecker;
import org.apache.cassandra.db.transform.Transformation;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.index.transactions.CompactionTransaction;
import org.apache.cassandra.index.transactions.IndexTransaction;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.metrics.TopPartitionTracker;
import org.apache.cassandra.schema.CompactionParams.TombstoneOption;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.paxos.PaxosRepairHistory;
import org.apache.cassandra.service.paxos.uncommitted.PaxosRows;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.TimeUUID;

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static org.apache.cassandra.config.Config.PaxosStatePurging.legacy;
import static org.apache.cassandra.config.DatabaseDescriptor.paxosStatePurging;

/**
 * Merge multiple iterators over the content of sstable into a "compacted" iterator.
 * <p>
 * On top of the actual merging the source iterators, this class:
 * <ul>
 *   <li>purge gc-able tombstones if possible (see PurgeIterator below).</li>
 *   <li>update 2ndary indexes if necessary (as we don't read-before-write on index updates, index entries are
 *       not deleted on deletion of the base table data, which is ok because we'll fix index inconsistency
 *       on reads. This however mean that potentially obsolete index entries could be kept a long time for
 *       data that is not read often, so compaction "pro-actively" fix such index entries. This is mainly
 *       an optimization).</li>
 *   <li>invalidate cached partitions that are empty post-compaction. This avoids keeping partitions with
 *       only purgable tombstones in the row cache.</li>
 *   <li>keep tracks of the compaction progress.</li>
 * </ul>
 */
public class CompactionIterator implements UnfilteredPartitionIterator
{
    private static final long UNFILTERED_TO_UPDATE_PROGRESS = 100;

    private final OperationType type;
    private final AbstractCompactionController controller;
    private final List<ISSTableScanner> scanners;
    private final ImmutableSet<SSTableReader> sstables;
    private final long nowInSec;
    private final TimeUUID compactionId;
    private final long totalBytes;
    private volatile long[] bytesReadByLevel;

    // Keep targetDirectory for compactions, needed for `nodetool compactionstats`
    private volatile String targetDirectory;

    /**
     * Merged frequency counters for partitions and rows (AKA histograms).
     * The array index represents the number of sstables containing the row or partition minus one. So index 0 contains
     * the number of rows or partitions coming from a single sstable (therefore copied rather than merged), index 1 contains
     * the number of rows or partitions coming from two sstables and so forth.
     */
    private final long[] mergedPartitionsHistogram;
    private final long[] mergedRowsHistogram;

    private final UnfilteredPartitionIterator compacted;
    private final TableOperation op;

    public CompactionIterator(OperationType type, List<ISSTableScanner> scanners, AbstractCompactionController controller, long nowInSec, TimeUUID compactionId)
    {
        this(type, scanners, controller, nowInSec, compactionId, null);
    }

    @SuppressWarnings("resource") // We make sure to close mergedIterator in close() and CompactionIterator is itself an AutoCloseable
    public CompactionIterator(OperationType type, List<ISSTableScanner> scanners, AbstractCompactionController controller, long nowInSec, TimeUUID compactionId, TopPartitionTracker.Collector topPartitionCollector)
    {
        this.controller = controller;
        this.type = type;
        this.scanners = scanners;
        this.nowInSec = nowInSec;
        this.compactionId = compactionId;
        this.bytesReadByLevel = new long[LeveledGenerations.MAX_LEVEL_COUNT];

        long bytes = 0;
        for (ISSTableScanner scanner : scanners)
            bytes += scanner.getLengthInBytes();
        this.totalBytes = bytes;
        this.mergedPartitionsHistogram = new long[scanners.size()];
        this.mergedRowsHistogram = new long[scanners.size()];
        // note that we leak `this` from the constructor when calling beginCompaction below, this means we have to get the sstables before
        // calling that to avoid a NPE.
        sstables = scanners.stream().map(ISSTableScanner::getBackingSSTables).flatMap(Collection::stream).collect(ImmutableSet.toImmutableSet());
        op = createOperation();

        UnfilteredPartitionIterator merged = scanners.isEmpty()
                                           ? EmptyIterators.unfilteredPartition(controller.realm.metadata())
                                           : UnfilteredPartitionIterators.merge(scanners, listener());
        if (topPartitionCollector != null) // need to count tombstones before they are purged
            merged = Transformation.apply(merged, new TopPartitionTracker.TombstoneCounter(topPartitionCollector, nowInSec));
        merged = Transformation.apply(merged, new GarbageSkipper(controller));
        Transformation<UnfilteredRowIterator> purger = isPaxos(controller.realm) && paxosStatePurging() != legacy
                                                       ? new PaxosPurger(nowInSec)
                                                       : new Purger(controller, nowInSec);
        merged = Transformation.apply(merged, purger);
        merged = DuplicateRowChecker.duringCompaction(merged, type);
        compacted = Transformation.apply(merged, new AbortableUnfilteredPartitionTransformation(op));
    }

    protected TableOperation createOperation()
    {
        return new AbstractTableOperation() {

            @Override
            public OperationProgress getProgress()
            {
                return new AbstractTableOperation.OperationProgress(controller.realm.metadata(), type, bytesRead(), totalBytes, getTotalBytesScanned(), compactionId, sstables);
            }

            @Override
            public boolean isGlobal()
            {
                return false;
            }
        };
    }

    /**
     * @return A {@link TableOperation} backed by this iterator. This operation can be observed for progress
     * and for interrupting provided that it is registered with a {@link TableOperationObserver}, normally the
     * metrics in the compaction manager. The caller is responsible for registering the operation and checking
     * {@link TableOperation#isStopRequested()}.
     */
    public TableOperation getOperation()
    {
        return op;
    }

    public TableMetadata metadata()
    {
        return controller.realm.metadata();
    }

    public long bytesRead()
    {
        long[] bytesReadByLevel = this.bytesReadByLevel;
        return Arrays.stream(bytesReadByLevel).reduce(Long::sum).orElse(0L);
    }

    long bytesRead(int level)
    {
        return level >= 0 && level < bytesReadByLevel.length ? bytesReadByLevel[level] : 0;
    }

    long totalBytes()
    {
        return totalBytes;
    }

    long totalSourcePartitions()
    {
        return Arrays.stream(mergedPartitionsHistogram).reduce(0L, Long::sum);
    }

    long totalSourceRows()
    {
        return Arrays.stream(mergedRowsHistogram).reduce(0L, Long::sum);
    }

    public long getTotalBytesScanned()
    {
        long bytesScanned = 0L;
        for (ISSTableScanner scanner : scanners)
            bytesScanned += scanner.getBytesScanned();

        return bytesScanned;
    }

    public long getTotalCompressedSize()
    {
        long compressedSize = 0;
        for (ISSTableScanner scanner : scanners)
            compressedSize += scanner.getCompressedLengthInBytes();

        return compressedSize;
    }

    public double getCompressionRatio()
    {
        double compressed = 0.0;
        double uncompressed = 0.0;

        for (ISSTableScanner scanner : scanners)
        {
            compressed += scanner.getCompressedLengthInBytes();
            uncompressed += scanner.getLengthInBytes();
        }

        if (compressed == uncompressed || uncompressed == 0)
            return MetadataCollector.NO_COMPRESSION_RATIO;

        return compressed / uncompressed;
    }

    long[] mergedPartitionsHistogram()
    {
        return mergedPartitionsHistogram;
    }

    long[] mergedRowsHistogram()
    {
        return mergedRowsHistogram;
    }

    public boolean isGlobal()
    {
        return false;
    }

    public void setTargetDirectory(final String targetDirectory)
    {
        this.targetDirectory = targetDirectory;
    }

    private UnfilteredPartitionIterators.MergeListener listener()
    {
        return new UnfilteredPartitionIterators.MergeListener()
        {
            private boolean rowProcessingNeeded()
            {
                return (type == OperationType.COMPACTION || type == OperationType.MAJOR_COMPACTION)
                       && controller.realm.getIndexManager().handles(IndexTransaction.Type.COMPACTION);
            }

            @Override
            public boolean preserveOrder()
            {
                return rowProcessingNeeded();
            }

            public UnfilteredRowIterators.MergeListener getRowMergeListener(DecoratedKey partitionKey, List<UnfilteredRowIterator> versions)
            {
                int numVersions = 0;
                for (int i=0, isize=versions.size(); i<isize; i++)
                {
                    UnfilteredRowIterator iter = versions.get(i);
                    if (iter != null)
                        numVersions++;
                }

                mergedPartitionsHistogram[numVersions - 1] += 1;

                final CompactionTransaction indexTransaction = rowProcessingNeeded() ? getIndexTransaction(partitionKey,versions)
                                                                                     : null;

                return new UnfilteredRowIterators.MergeListener()
                {
                    @Override
                    public void onMergedPartitionLevelDeletion(DeletionTime mergedDeletion, DeletionTime[] versions) {}

                    @Override
                    public void onMergedRows(Row merged, Row[] versions)
                    {
                        int numVersions = 0;
                        for (Row v : versions)
                        {
                            if (v != null)
                                numVersions++;
                        }

                        assert numVersions > 0 && numVersions - 1 < mergedRowsHistogram.length;
                        mergedRowsHistogram[numVersions - 1] += 1;

                        if (indexTransaction != null)
                        {
                            indexTransaction.start();
                            indexTransaction.onRowMerge(merged, versions);
                            indexTransaction.commit();
                        }

                    }

                    @Override
                    public void onMergedRangeTombstoneMarkers(RangeTombstoneMarker mergedMarker, RangeTombstoneMarker[] versions) {}

                    @Override
                    public void close() {}
                };
            }
        };
    }

    private CompactionTransaction getIndexTransaction(DecoratedKey partitionKey, List<UnfilteredRowIterator> versions)
    {
        Columns statics = Columns.NONE;
        Columns regulars = Columns.NONE;
        for (int i=0, isize=versions.size(); i<isize; i++)
        {
            @SuppressWarnings("resource")
            UnfilteredRowIterator iter = versions.get(i);
            if (iter != null)
            {
                statics = statics.mergeTo(iter.columns().statics);
                regulars = regulars.mergeTo(iter.columns().regulars);
            }
        }
        final RegularAndStaticColumns regularAndStaticColumns = new RegularAndStaticColumns(statics, regulars);
        // If we have a 2ndary index, we must update it with deleted/shadowed cells.
        // we can reuse a single CleanupTransaction for the duration of a partition.
        // Currently, it doesn't do any batching of row updates, so every merge event
        // for a single partition results in a fresh cycle of:
        // * Get new Indexer instances
        // * Indexer::start
        // * Indexer::onRowMerge (for every row being merged by the compaction)
        // * Indexer::commit
        // A new OpOrder.Group is opened in an ARM block wrapping the commits
        // TODO: this should probably be done asynchronously and batched.
        return controller.realm.getIndexManager().newCompactionTransaction(partitionKey, regularAndStaticColumns, versions.size(), nowInSec);
    }

    private void updateBytesRead()
    {
        long[] bytesReadByLevel = new long[this.bytesReadByLevel.length];
        for (ISSTableScanner scanner : scanners)
        {
            int level = scanner.level();
            long n = scanner.getCurrentPosition();

            if (level >= 0 && level < bytesReadByLevel.length)
                bytesReadByLevel[level] += n;
        }
        this.bytesReadByLevel = bytesReadByLevel;
    }

    public boolean hasNext()
    {
        return compacted.hasNext();
    }

    public UnfilteredRowIterator next()
    {
        return compacted.next();
    }

    public void remove()
    {
        throw new UnsupportedOperationException();
    }

    public void close()
    {
        updateBytesRead();

        Throwables.maybeFail(Throwables.close(null, compacted));
    }

    public String toString()
    {
        return String.format("%s: %s, (%d/%d)", type, metadata(), bytesRead(), totalBytes());
    }

    class Purger extends PurgeFunction
    {
        private final AbstractCompactionController controller;

        private DecoratedKey currentKey;
        private LongPredicate purgeEvaluator;

        private long compactedUnfiltered;

        private Purger(AbstractCompactionController controller, long nowInSec)
        {
            super(nowInSec, controller.gcBefore, controller.compactingRepaired() ? Long.MAX_VALUE : Integer.MIN_VALUE,
                  controller.realm.onlyPurgeRepairedTombstones(),
                  controller.realm.metadata().enforceStrictLiveness());
            this.controller = controller;
        }

        @Override
        protected void onEmptyPartitionPostPurge(DecoratedKey key)
        {
            if (type == OperationType.COMPACTION)
                controller.realm.invalidateCachedPartition(key);
        }

        @Override
        protected void onNewPartition(DecoratedKey key)
        {
            currentKey = key;
            purgeEvaluator = null;
        }

        @Override
        protected void updateProgress()
        {
            if ((++compactedUnfiltered) % UNFILTERED_TO_UPDATE_PROGRESS == 0)
                updateBytesRead();
        }

        /*
         * Called at the beginning of each new partition
         * Return true if the current partitionKey ignores the gc_grace_seconds during compaction.
         * Note that this method should be called after the onNewPartition because it depends on the currentKey
         * which is set in the onNewPartition
         */
        @Override
        protected boolean shouldIgnoreGcGrace()
        {
            return controller.realm.shouldIgnoreGcGraceForKey(currentKey);
        }

        /*
         * Evaluates whether a tombstone with the given deletion timestamp can be purged. This is the minimum
         * timestamp for any sstable containing `currentKey` outside of the set of sstables involved in this compaction.
         * This is computed lazily on demand as we only need this if there is tombstones and this a bit expensive
         * (see #8914).
         */
        protected LongPredicate getPurgeEvaluator()
        {
            if (purgeEvaluator == null)
            {
                purgeEvaluator = controller.getPurgeEvaluator(currentKey);
            }
            return purgeEvaluator;
        }
    }

    /**
     * Unfiltered row iterator that removes deleted data as provided by a "tombstone source" for the partition.
     * The result produced by this iterator is such that when merged with tombSource it produces the same output
     * as the merge of dataSource and tombSource.
     */
    private static class GarbageSkippingUnfilteredRowIterator implements WrappingUnfilteredRowIterator
    {
        private final UnfilteredRowIterator wrapped;

        final UnfilteredRowIterator tombSource;
        final DeletionTime partitionLevelDeletion;
        final Row staticRow;
        final ColumnFilter cf;
        final TableMetadata metadata;
        final boolean cellLevelGC;

        DeletionTime tombOpenDeletionTime = DeletionTime.LIVE;
        DeletionTime dataOpenDeletionTime = DeletionTime.LIVE;
        DeletionTime openDeletionTime = DeletionTime.LIVE;
        DeletionTime partitionDeletionTime;
        DeletionTime activeDeletionTime;
        Unfiltered tombNext;
        Unfiltered dataNext;
        Unfiltered next = null;

        /**
         * Construct an iterator that filters out data shadowed by the provided "tombstone source".
         *
         * @param dataSource The input row. The result is a filtered version of this.
         * @param tombSource Tombstone source, i.e. iterator used to identify deleted data in the input row.
         * @param cellLevelGC If false, the iterator will only look at row-level deletion times and tombstones.
         *                    If true, deleted or overwritten cells within a surviving row will also be removed.
         */
        protected GarbageSkippingUnfilteredRowIterator(UnfilteredRowIterator dataSource, UnfilteredRowIterator tombSource, boolean cellLevelGC)
        {
            this.wrapped = dataSource;
            this.tombSource = tombSource;
            this.cellLevelGC = cellLevelGC;
            metadata = dataSource.metadata();
            cf = ColumnFilter.all(metadata);

            activeDeletionTime = partitionDeletionTime = tombSource.partitionLevelDeletion();

            // Only preserve partition level deletion if not shadowed. (Note: Shadowing deletion must not be copied.)
            this.partitionLevelDeletion = dataSource.partitionLevelDeletion().supersedes(tombSource.partitionLevelDeletion()) ?
                    dataSource.partitionLevelDeletion() :
                    DeletionTime.LIVE;

            Row dataStaticRow = garbageFilterRow(dataSource.staticRow(), tombSource.staticRow());
            this.staticRow = dataStaticRow != null ? dataStaticRow : Rows.EMPTY_STATIC_ROW;

            tombNext = advance(tombSource);
            dataNext = advance(dataSource);
        }

        @Override
        public UnfilteredRowIterator wrapped()
        {
            return wrapped;
        }

        private static Unfiltered advance(UnfilteredRowIterator source)
        {
            return source.hasNext() ? source.next() : null;
        }

        @Override
        public DeletionTime partitionLevelDeletion()
        {
            return partitionLevelDeletion;
        }

        public void close()
        {
            wrapped.close();
            tombSource.close();
        }

        @Override
        public Row staticRow()
        {
            return staticRow;
        }

        @Override
        public boolean hasNext()
        {
            // Produce the next element. This may consume multiple elements from both inputs until we find something
            // from dataSource that is still live. We track the currently open deletion in both sources, as well as the
            // one we have last issued to the output. The tombOpenDeletionTime is used to filter out content; the others
            // to decide whether or not a tombstone is superseded, and to be able to surface (the rest of) a deletion
            // range from the input when a suppressing deletion ends.
            while (next == null && dataNext != null)
            {
                int cmp = tombNext == null ? -1 : metadata.comparator.compare(dataNext, tombNext);
                if (cmp < 0)
                {
                    if (dataNext.isRow())
                        next = ((Row) dataNext).filter(cf, activeDeletionTime, false, metadata);
                    else
                        next = processDataMarker();
                }
                else if (cmp == 0)
                {
                    if (dataNext.isRow())
                    {
                        next = garbageFilterRow((Row) dataNext, (Row) tombNext);
                    }
                    else
                    {
                        tombOpenDeletionTime = updateOpenDeletionTime(tombOpenDeletionTime, tombNext);
                        activeDeletionTime = Ordering.natural().max(partitionDeletionTime,
                                                                    tombOpenDeletionTime);
                        next = processDataMarker();
                    }
                }
                else // (cmp > 0)
                {
                    if (tombNext.isRangeTombstoneMarker())
                    {
                        tombOpenDeletionTime = updateOpenDeletionTime(tombOpenDeletionTime, tombNext);
                        activeDeletionTime = Ordering.natural().max(partitionDeletionTime,
                                                                    tombOpenDeletionTime);
                        boolean supersededBefore = openDeletionTime.isLive();
                        boolean supersededAfter = !dataOpenDeletionTime.supersedes(activeDeletionTime);
                        // If a range open was not issued because it was superseded and the deletion isn't superseded anymore, we need to open it now.
                        if (supersededBefore && !supersededAfter)
                            next = new RangeTombstoneBoundMarker(((RangeTombstoneMarker) tombNext).closeBound(false).invert(), dataOpenDeletionTime);
                        // If the deletion begins to be superseded, we don't close the range yet. This can save us a close/open pair if it ends after the superseding range.
                    }
                }

                if (next instanceof RangeTombstoneMarker)
                    openDeletionTime = updateOpenDeletionTime(openDeletionTime, next);

                if (cmp <= 0)
                    dataNext = advance(wrapped);
                if (cmp >= 0)
                    tombNext = advance(tombSource);
            }
            return next != null;
        }

        protected Row garbageFilterRow(Row dataRow, Row tombRow)
        {
            if (cellLevelGC)
            {
                return Rows.removeShadowedCells(dataRow, tombRow, activeDeletionTime);
            }
            else
            {
                DeletionTime deletion = Ordering.natural().max(tombRow.deletion().time(),
                                                               activeDeletionTime);
                return dataRow.filter(cf, deletion, false, metadata);
            }
        }

        /**
         * Decide how to act on a tombstone marker from the input iterator. We can decide what to issue depending on
         * whether or not the ranges before and after the marker are superseded/live -- if none are, we can reuse the
         * marker; if both are, the marker can be ignored; otherwise we issue a corresponding start/end marker.
         */
        private RangeTombstoneMarker processDataMarker()
        {
            dataOpenDeletionTime = updateOpenDeletionTime(dataOpenDeletionTime, dataNext);
            boolean supersededBefore = openDeletionTime.isLive();
            boolean supersededAfter = !dataOpenDeletionTime.supersedes(activeDeletionTime);
            RangeTombstoneMarker marker = (RangeTombstoneMarker) dataNext;
            if (!supersededBefore)
                if (!supersededAfter)
                    return marker;
                else
                    return new RangeTombstoneBoundMarker(marker.closeBound(false), marker.closeDeletionTime(false));
            else
                if (!supersededAfter)
                    return new RangeTombstoneBoundMarker(marker.openBound(false), marker.openDeletionTime(false));
                else
                    return null;
        }

        @Override
        public Unfiltered next()
        {
            if (!hasNext())
                throw new IllegalStateException();

            Unfiltered v = next;
            next = null;
            return v;
        }

        private DeletionTime updateOpenDeletionTime(DeletionTime openDeletionTime, Unfiltered next)
        {
            RangeTombstoneMarker marker = (RangeTombstoneMarker) next;
            assert openDeletionTime.isLive() == !marker.isClose(false);
            assert openDeletionTime.isLive() || openDeletionTime.equals(marker.closeDeletionTime(false));
            return marker.isOpen(false) ? marker.openDeletionTime(false) : DeletionTime.LIVE;
        }
    }

    /**
     * Partition transformation applying GarbageSkippingUnfilteredRowIterator, obtaining tombstone sources for each
     * partition using the controller's shadowSources method.
     */
    private static class GarbageSkipper extends Transformation<UnfilteredRowIterator>
    {
        final AbstractCompactionController controller;
        final boolean cellLevelGC;

        private GarbageSkipper(AbstractCompactionController controller)
        {
            this.controller = controller;
            cellLevelGC = controller.tombstoneOption == TombstoneOption.CELL;
        }

        @Override
        protected UnfilteredRowIterator applyToPartition(UnfilteredRowIterator partition)
        {
            Iterable<UnfilteredRowIterator> sources = controller.shadowSources(partition.partitionKey(), !cellLevelGC);
            if (sources == null)
                return partition;
            List<UnfilteredRowIterator> iters = new ArrayList<>();
            for (UnfilteredRowIterator iter : sources)
            {
                if (!iter.isEmpty())
                    iters.add(iter);
                else
                    iter.close();
            }
            if (iters.isEmpty())
                return partition;

            return new GarbageSkippingUnfilteredRowIterator(partition, UnfilteredRowIterators.merge(iters), cellLevelGC);
        }
    }

    private class PaxosPurger extends Transformation<UnfilteredRowIterator>
    {
        private final long nowInSec;
        private final long paxosPurgeGraceMicros = DatabaseDescriptor.getPaxosPurgeGrace(MICROSECONDS);
        private final Map<TableId, PaxosRepairHistory.Searcher> tableIdToHistory = new HashMap<>();
        private Token currentToken;
        private int compactedUnfiltered;

        private PaxosPurger(long nowInSec)
        {
            this.nowInSec = nowInSec;
        }

        protected void onEmptyPartitionPostPurge(DecoratedKey key)
        {
            if (type == OperationType.COMPACTION)
                controller.realm.invalidateCachedPartition(key);
        }

        protected void updateProgress()
        {
            if ((++compactedUnfiltered) % UNFILTERED_TO_UPDATE_PROGRESS == 0)
                updateBytesRead();
        }

        @Override
        protected UnfilteredRowIterator applyToPartition(UnfilteredRowIterator partition)
        {
            currentToken = partition.partitionKey().getToken();
            UnfilteredRowIterator purged = Transformation.apply(partition, this);
            if (purged.isEmpty())
            {
                onEmptyPartitionPostPurge(purged.partitionKey());
                purged.close();
                return null;
            }

            return purged;
        }

        @Override
        protected Row applyToRow(Row row)
        {
            updateProgress();
            TableId tableId = PaxosRows.getTableId(row);

            switch (paxosStatePurging())
            {
                default: throw new AssertionError();
                case legacy:
                case gc_grace:
                {
                    TableMetadata metadata = Schema.instance.getTableMetadata(tableId);
                    return row.purgeDataOlderThan(TimeUnit.SECONDS.toMicros(nowInSec - (metadata == null ? (3 * 3600) : metadata.params.gcGraceSeconds)), false);
                }
                case repaired:
                {
                    PaxosRepairHistory.Searcher history = tableIdToHistory.computeIfAbsent(tableId, find -> {
                        TableMetadata metadata = Schema.instance.getTableMetadata(find);
                        if (metadata == null)
                            return null;
                        return Keyspace.openAndGetStore(metadata).getPaxosRepairHistory().searcher();
                    });

                    return history == null ? row :
                           row.purgeDataOlderThan(history.ballotForToken(currentToken).unixMicros() - paxosPurgeGraceMicros, false);
                }
            }
        }
    }

    private static class AbortableUnfilteredPartitionTransformation extends Transformation<UnfilteredRowIterator>
    {
        private final AbortableUnfilteredRowTransformation abortableIter;
        private final TableOperation op;

        private AbortableUnfilteredPartitionTransformation(TableOperation op)
        {
            this.op = op;
            this.abortableIter = new AbortableUnfilteredRowTransformation(op);
        }

        @Override
        protected UnfilteredRowIterator applyToPartition(UnfilteredRowIterator partition)
        {
            op.throwIfStopRequested();
            return Transformation.apply(partition, abortableIter);
        }
    }

    private static class AbortableUnfilteredRowTransformation extends Transformation<UnfilteredRowIterator>
    {
        private final TableOperation op;

        private AbortableUnfilteredRowTransformation(TableOperation op)
        {
            this.op = op;
        }

        public Row applyToRow(Row row)
        {
            op.throwIfStopRequested();
            return row;
        }
    }

    private static boolean isPaxos(CompactionRealm realm)
    {
        return realm.getTableName().equals(SystemKeyspace.PAXOS) && realm.getKeyspaceName().equals(SchemaConstants.SYSTEM_KEYSPACE_NAME);
    }
}
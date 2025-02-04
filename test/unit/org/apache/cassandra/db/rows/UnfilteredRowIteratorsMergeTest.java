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
package org.apache.cassandra.db.rows;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.function.IntUnaryOperator;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.BufferClusteringBound;
import org.apache.cassandra.db.Clusterable;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.ClusteringBoundary;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.rows.Unfiltered.Kind;
import org.apache.cassandra.io.sstable.compaction.IteratorFromCursor;
import org.apache.cassandra.io.sstable.compaction.SSTableCursor;
import org.apache.cassandra.io.sstable.compaction.SSTableCursorMerger;
import org.apache.cassandra.io.sstable.compaction.SkipEmptyDataCursor;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.FBUtilities;

public class UnfilteredRowIteratorsMergeTest
{
    static
    {
        DatabaseDescriptor.daemonInitialization();
    }
    static DecoratedKey partitionKey = Util.dk("key");
    static Map<List<Unfiltered>, DeletionTime> partitionLevelDeletions = Maps.newHashMap();
    static TableMetadata metadata =
        TableMetadata.builder("UnfilteredRowIteratorsMergeTest", "Test")
                     .addPartitionKeyColumn("key", AsciiType.instance)
                     .addClusteringColumn("clustering", Int32Type.instance)
                     .addRegularColumn("data", Int32Type.instance)
                     .build();

    static Comparator<Clusterable> comparator = new ClusteringComparator(Int32Type.instance);
    static long nowInSec = FBUtilities.nowInSeconds();

    static final int RANGE = 3000;
    static final int DEL_RANGE = 100;
    static final int ITERATORS = 15;
    static final int ITEMS = 300;

    boolean reversed;

    public UnfilteredRowIteratorsMergeTest()
    {
    }

    @Test
    public void testTombstoneMerge()
    {
        testTombstoneMerge(false, false, false);
    }

    @Test
    public void testTombstoneMergeReversed()
    {
        testTombstoneMerge(true, false, false);
    }
    @Test
    public void testTombstoneMergeCursor()
    {
        testTombstoneMerge(false, false, true);
    }


    @Test
    public void testTombstoneMergeIterative()
    {
        testTombstoneMerge(false, true, false);
    }

    @Test
    public void testTombstoneMergeReversedIterative()
    {
        testTombstoneMerge(true, true, false);
    }

    @Test
    public void testTombstoneMergeCursorIterative()
    {
        testTombstoneMerge(false, true, true);
    }

    @Test
    public void testDuplicateRangeCase()
    {
        testForInput("67<=[98] [98]<=67",
                     "66<=[11] [11]<71",
                     "66<[13] [13]<67");
    }

    @Test
    public void testWithPartitionLevelDeletion()
    {
        testForInput("D5|68[7]",
                     "67<=[11] [11]<69" );
    }

    @SuppressWarnings("unused")
    public void testTombstoneMerge(boolean reversed, boolean iterations, boolean throughCursor)
    {
        this.reversed = reversed;
        UnfilteredRowsGenerator generator = new UnfilteredRowsGenerator(comparator, reversed);

        for (int seed = 1; seed <= 100; ++seed)
        {
            if (ITEMS <= 20)
                System.out.println("\nSeed " + seed);

            Random r = new Random(seed);
            List<IntUnaryOperator> timeGenerators = ImmutableList.of(
                    x -> -1,
                    x -> DEL_RANGE,
                    x -> r.nextInt(DEL_RANGE)
                );
            List<List<Unfiltered>> sources = new ArrayList<>(ITERATORS);
            if (ITEMS <= 20)
                System.out.println("Merging");
            for (int i=0; i<ITERATORS; ++i)
                sources.add(generator.generateSource(r, ITEMS, RANGE, DEL_RANGE, timeGenerators.get(r.nextInt(timeGenerators.size()))));
            List<Unfiltered> merged = merge(sources, iterations, throughCursor);

            if (ITEMS <= 20)
                System.out.println("results in");
            if (ITEMS <= 20)
                generator.dumpList(merged);
            verifyEquivalent(sources, merged, generator);
            generator.verifyValid(merged);
            if (reversed)
            {
                Collections.reverse(merged);
                generator.verifyValid(merged, false);
            }
        }
    }

    private List<Unfiltered> merge(List<List<Unfiltered>> sources, boolean iterations, boolean throughCursors)
    {
        if (throughCursors)
            return mergeThroughCursors(sources,  iterations);
        else
            return mergeThroughIterators(sources, iterations);
    }

    private List<Unfiltered> mergeThroughIterators(List<List<Unfiltered>> sources, boolean iterations)
    {
        List<UnfilteredRowIterator> us = sources.
                stream().
                map(l -> new UnfilteredRowsGenerator.Source(l.iterator(),
                                                            metadata,
                                                            partitionKey,
                                                            partitionLevelDeletions.computeIfAbsent(l, v -> DeletionTime.LIVE),
                                                            reversed)).
                collect(Collectors.toList());
        List<Unfiltered> merged = new ArrayList<>();
        Iterators.addAll(merged, mergeIterators(us, iterations));
        return merged;
    }

    public UnfilteredRowIterator mergeIterators(List<UnfilteredRowIterator> us, boolean iterations)
    {
        if (iterations)
        {
            UnfilteredRowIterator mi = us.get(0);
            int i;
            for (i = 1; i + 2 <= ITERATORS; i += 2)
                mi = UnfilteredRowIterators.merge(ImmutableList.of(mi, us.get(i), us.get(i+1)));
            if (i + 1 <= ITERATORS)
                mi = UnfilteredRowIterators.merge(ImmutableList.of(mi, us.get(i)));
            return mi;
        }
        else
        {
            return UnfilteredRowIterators.merge(us);
        }
    }

    private List<Unfiltered> mergeThroughCursors(List<List<Unfiltered>> sources, boolean iterations)
    {
        List<SSTableCursor> us = sources.stream()
                                        .map(l -> cursor(l,
                                                         partitionKey,
                                                         partitionLevelDeletions.computeIfAbsent(l, v -> DeletionTime.LIVE)))
                                        .collect(Collectors.toList());
        List<Unfiltered> merged = new ArrayList<>();
        Iterators.addAll(merged, new IteratorFromCursor(metadata, mergeCursors(us, iterations)).next());
        return merged;
    }

    private SSTableCursor mergeCursors(List<SSTableCursor> us, boolean iterations)
    {
        if (iterations)
        {
            SSTableCursor mi = us.get(0);
            int i;
            for (i = 1; i + 2 <= ITERATORS; i += 2)
                mi = new SSTableCursorMerger(ImmutableList.of(mi, us.get(i), us.get(i+1)), metadata);
            if (i + 1 <= ITERATORS)
                mi = new SSTableCursorMerger(ImmutableList.of(mi, us.get(i)), metadata);
            return new SkipEmptyDataCursor(mi);
        }
        else
        {
            return new SkipEmptyDataCursor(new SSTableCursorMerger(us, metadata));
        }
    }

    private SSTableCursor cursor(List<Unfiltered> content, DecoratedKey partitionKey, DeletionTime partitionLevelDeletion)
    {
        return new SSTableCursor() {
            Type type = Type.UNINITIALIZED;
            Iterator<Unfiltered> iterator = content.iterator();
            DeletionTime activeRangeDeletion = DeletionTime.LIVE;
            DeletionTime rowLevelDeletion;
            Unfiltered current;
            Iterator<Cell<?>> cellIterator;
            Cell currentCell;

            public Type advance()
            {
                if (type == Type.RANGE_TOMBSTONE)
                    activeRangeDeletion = rowLevelDeletion;

                switch (type)
                {
                    case UNINITIALIZED:
                        return type = Type.PARTITION;
                    case ROW:
                    case SIMPLE_COLUMN:
                        if (cellIterator.hasNext())
                        {
                            currentCell = cellIterator.next();
                            return type = Type.SIMPLE_COLUMN;
                        }
                        // else fall through
                    case RANGE_TOMBSTONE:
                    case PARTITION:
                        if (!iterator.hasNext())
                            return type = Type.EXHAUSTED;

                        current = iterator.next();
                        if (current.isRow())
                        {
                            Row row = (Row) this.current;
                            cellIterator = row.cells().iterator();
                            rowLevelDeletion = row.deletion().time();
                            return type = Type.ROW;
                        }
                        else
                        {
                            RangeTombstoneMarker marker = (RangeTombstoneMarker) this.current;
                            rowLevelDeletion = marker.isOpen(false) ? marker.openDeletionTime(false) : DeletionTime.LIVE;
                            return type = Type.RANGE_TOMBSTONE;
                        }
                    default:
                        throw new AssertionError();
                }
            }

            public Type type()
            {
                return type;
            }

            public DecoratedKey partitionKey()
            {
                return partitionKey;
            }

            public DeletionTime partitionLevelDeletion()
            {
                return partitionLevelDeletion;
            }

            public ClusteringPrefix clusteringKey()
            {
                return current.clustering();
            }

            public LivenessInfo clusteringKeyLivenessInfo()
            {
                return ((Row) current).primaryKeyLivenessInfo();
            }

            public DeletionTime rowLevelDeletion()
            {
                return rowLevelDeletion;
            }

            public DeletionTime activeRangeDeletion()
            {
                return activeRangeDeletion;
            }

            public DeletionTime complexColumnDeletion()
            {
                return null;    // we don't return complex columns
            }

            public ColumnMetadata column()
            {
                return cell().column;
            }

            public long bytesProcessed()
            {
                return 0;
            }

            public long bytesTotal()
            {
                return 0;
            }

            public Cell cell()
            {
                return currentCell;
            }

            public void close()
            {
                // nothing
            }
        };
    }

    @SuppressWarnings("unused")
    private List<Unfiltered> generateSource(Random r, IntUnaryOperator timeGenerator)
    {
        int[] positions = new int[ITEMS + 1];
        for (int i=0; i<ITEMS; ++i)
            positions[i] = r.nextInt(RANGE);
        positions[ITEMS] = RANGE;
        Arrays.sort(positions);

        List<Unfiltered> content = new ArrayList<>(ITEMS);
        int prev = -1;
        for (int i=0; i<ITEMS; ++i)
        {
            int pos = positions[i];
            int sz = positions[i + 1] - pos;
            if (sz == 0 && pos == prev)
                // Filter out more than two of the same position.
                continue;
            if (r.nextBoolean() || pos == prev)
            {
                int span;
                boolean includesStart;
                boolean includesEnd;
                if (pos > prev)
                {
                    span = r.nextInt(sz + 1);
                    includesStart = span > 0 ? r.nextBoolean() : true;
                    includesEnd = span > 0 ? r.nextBoolean() : true;
                }
                else
                {
                    span = 1 + r.nextInt(sz);
                    includesStart = false;
                    includesEnd = r.nextBoolean();
                }
                long deltime = r.nextInt(DEL_RANGE);
                DeletionTime dt = DeletionTime.build(deltime, deltime);
                content.add(new RangeTombstoneBoundMarker(boundFor(pos, true, includesStart), dt));
                content.add(new RangeTombstoneBoundMarker(boundFor(pos + span, false, includesEnd), dt));
                prev = pos + span - (includesEnd ? 0 : 1);
            }
            else
            {
                content.add(emptyRowAt(pos, timeGenerator));
                prev = pos;
            }
        }

        attachBoundaries(content);
        if (reversed)
        {
            Collections.reverse(content);
        }
        verifyValid(content);
        if (ITEMS <= 20)
            dumpList(content);
        return content;
    }

    static void attachBoundaries(List<Unfiltered> content)
    {
        int di = 0;
        RangeTombstoneMarker prev = null;
        for (int si = 0; si < content.size(); ++si)
        {
            Unfiltered currUnfiltered = content.get(si);
            RangeTombstoneMarker curr = currUnfiltered.kind() == Kind.RANGE_TOMBSTONE_MARKER ?
                                        (RangeTombstoneMarker) currUnfiltered :
                                        null;
            if (prev != null && curr != null && prev.isClose(false) && curr.isOpen(false) && prev.clustering().invert().equals(curr.clustering()))
            {
                // Join. Prefer not to use merger to check its correctness.
                ClusteringBound<?> b = ((RangeTombstoneBoundMarker) prev).clustering();
                ClusteringBoundary boundary = ClusteringBoundary.create(b.isInclusive()
                                                                        ? ClusteringPrefix.Kind.INCL_END_EXCL_START_BOUNDARY
                                                                        : ClusteringPrefix.Kind.EXCL_END_INCL_START_BOUNDARY,
                                                                        b);
                prev = new RangeTombstoneBoundaryMarker(boundary, prev.closeDeletionTime(false), curr.openDeletionTime(false));
                currUnfiltered = prev;
                --di;
            }
            content.set(di++, currUnfiltered);
            prev = curr;
        }
        for (int pos = content.size() - 1; pos >= di; --pos)
            content.remove(pos);
    }

    void verifyValid(List<Unfiltered> list)
    {
        int reversedAsMultiplier = reversed ? -1 : 1;
        try {
            RangeTombstoneMarker prev = null;
            Unfiltered prevUnfiltered = null;
            for (Unfiltered unfiltered : list)
            {
                Assert.assertTrue("Order violation prev " + str(prevUnfiltered) + " curr " + str(unfiltered),
                                  prevUnfiltered == null || comparator.compare(prevUnfiltered, unfiltered) * reversedAsMultiplier < 0);
                prevUnfiltered = unfiltered;

                if (unfiltered.kind() == Kind.RANGE_TOMBSTONE_MARKER)
                {
                    RangeTombstoneMarker curr = (RangeTombstoneMarker) unfiltered;
                    if (prev != null)
                    {
                        if (curr.isClose(reversed))
                        {
                            Assert.assertTrue(str(unfiltered) + " follows another close marker " + str(prev), prev.isOpen(reversed));
                            Assert.assertEquals("Deletion time mismatch for open " + str(prev) + " and close " + str(unfiltered),
                                                prev.openDeletionTime(reversed),
                                                curr.closeDeletionTime(reversed));
                        }
                        else
                            Assert.assertFalse(str(curr) + " follows another open marker " + str(prev), prev.isOpen(reversed));
                    }

                    prev = curr;
                }
            }
            Assert.assertFalse("Cannot end in open marker " + str(prev), prev != null && prev.isOpen(reversed));

        } catch (AssertionError e) {
            System.out.println(e);
            dumpList(list);
            throw e;
        }
    }

    void verifyEquivalent(List<List<Unfiltered>> sources, List<Unfiltered> merged, UnfilteredRowsGenerator generator)
    {
        try
        {
            for (int i=0; i<RANGE; ++i)
            {
                Clusterable c = UnfilteredRowsGenerator.clusteringFor(i);
                DeletionTime dt = DeletionTime.LIVE;
                for (List<Unfiltered> source : sources)
                {
                    dt = deletionFor(c, source, dt);
                }
                Assert.assertEquals("Deletion time mismatch for position " + i, dt, deletionFor(c, merged));
                if (dt == DeletionTime.LIVE)
                {
                    Optional<Unfiltered> sourceOpt = sources.stream().map(source -> rowFor(c, source)).filter(x -> x != null).findAny();
                    Unfiltered mergedRow = rowFor(c, merged);
                    Assert.assertEquals("Content mismatch for position " + i, clustering(sourceOpt.orElse(null)), clustering(mergedRow));
                }
            }
        }
        catch (AssertionError e)
        {
            System.out.println(e);
            for (List<Unfiltered> list : sources)
                generator.dumpList(list);
            System.out.println("merged");
            generator.dumpList(merged);
            throw e;
        }
    }

    String clustering(Clusterable curr)
    {
        if (curr == null)
            return "null";
        return Int32Type.instance.getString(curr.clustering().bufferAt(0));
    }

    private Unfiltered rowFor(Clusterable pointer, List<Unfiltered> list)
    {
        int index = Collections.binarySearch(list, pointer, reversed ? comparator.reversed() : comparator);
        return index >= 0 ? list.get(index) : null;
    }

    DeletionTime deletionFor(Clusterable pointer, List<Unfiltered> list)
    {
        return deletionFor(pointer, list, DeletionTime.LIVE);
    }

    DeletionTime deletionFor(Clusterable pointer, List<Unfiltered> list, DeletionTime def)
    {
        if (list.isEmpty())
            return def;

        int index = Collections.binarySearch(list, pointer, reversed ? comparator.reversed() : comparator);
        if (index < 0)
            index = -1 - index;
        else
        {
            Row row = (Row) list.get(index);
            if (row.deletion().supersedes(def))
                def = row.deletion().time();
        }

        if (index >= list.size())
            return def;

        while (--index >= 0)
        {
            Unfiltered unfiltered = list.get(index);
            if (unfiltered.kind() == Kind.ROW)
                continue;
            RangeTombstoneMarker lower = (RangeTombstoneMarker) unfiltered;
            if (!lower.isOpen(reversed))
                return def;
            return lower.openDeletionTime(reversed).supersedes(def) ? lower.openDeletionTime(reversed) : def;
        }
        return def;
    }

    private static ClusteringBound<?> boundFor(int pos, boolean start, boolean inclusive)
    {
        return BufferClusteringBound.create(ClusteringBound.boundKind(start, inclusive), new ByteBuffer[] {Int32Type.instance.decompose(pos)});
    }

    private static Clustering<?> clusteringFor(int i)
    {
        return Clustering.make(Int32Type.instance.decompose(i));
    }

    static Row emptyRowAt(int pos, IntUnaryOperator timeGenerator)
    {
        final Clustering<?> clustering = clusteringFor(pos);
        final LivenessInfo live = LivenessInfo.create(timeGenerator.applyAsInt(pos), nowInSec);
        return BTreeRow.noCellLiveRow(clustering, live);
    }

    private void dumpList(List<Unfiltered> list)
    {
        for (Unfiltered u : list)
            System.out.print(str(u) + " ");
        System.out.println();
    }

    private String str(Clusterable curr)
    {
        if (curr == null)
            return "null";
        String val = Int32Type.instance.getString(curr.clustering().bufferAt(0));
        if (curr instanceof RangeTombstoneMarker)
        {
            RangeTombstoneMarker marker = (RangeTombstoneMarker) curr;
            if (marker.isClose(reversed))
                val = "[" + marker.closeDeletionTime(reversed).markedForDeleteAt() + "]" + (marker.closeIsInclusive(reversed) ? "<=" : "<") + val;
            if (marker.isOpen(reversed))
                val = val + (marker.openIsInclusive(reversed) ? "<=" : "<") + "[" + marker.openDeletionTime(reversed).markedForDeleteAt() + "]";
        }
        return val;
    }

    public void testForInput(String... inputs)
    {
        reversed = false;
        UnfilteredRowsGenerator generator = new UnfilteredRowsGenerator(comparator, false);

        List<List<Unfiltered>> sources = new ArrayList<>();
        for (String input : inputs)
        {
            List<Unfiltered> source = generator.parse(input, DEL_RANGE, partitionLevelDeletions);
            generator.dumpList(source);
            generator.verifyValid(source);
            sources.add(source);
        }

        List<Unfiltered> merged = merge(sources, false, false);
        System.out.println("Merge through iterator to:");
        generator.dumpList(merged);
        verifyEquivalent(sources, merged, generator);
        generator.verifyValid(merged);
        System.out.println();
        merged = merge(sources, false, true);
        System.out.println("Merge through cursors to:");
        generator.dumpList(merged);
        verifyEquivalent(sources, merged, generator);
        generator.verifyValid(merged);
        System.out.println();
    }
}

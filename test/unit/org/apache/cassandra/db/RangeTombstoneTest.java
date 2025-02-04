/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.db;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.UpdateBuilder;
import org.apache.cassandra.Util;
import org.apache.cassandra.cql3.statements.schema.IndexTarget;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.partitions.FilteredPartition;
import org.apache.cassandra.db.partitions.ImmutableBTreePartition;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.index.StubIndex;
import org.apache.cassandra.io.sstable.SSTableReadsListener;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.SchemaTestUtil;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.OrderCheckingIterator;

import static org.apache.cassandra.SchemaLoader.standardCFMD;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class RangeTombstoneTest
{
    private static final String KSNAME = "RangeTombstoneTest";
    private static final String CFNAME = "StandardInteger1";
    private static final String CFNAME_INDEXED = "StandardIntegerIndexed";
    public static final int GC_GRACE = 5000;

    @Parameterized.Parameters(name = "compaction={0}")
    public static Iterable<CompactionParams> compactionParamSets()
    {
        return ImmutableSet.of(CompactionParams.stcs(ImmutableMap.of()),
                               CompactionParams.ucs(ImmutableMap.of()));
    }

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KSNAME,
                                    KeyspaceParams.simple(1),
                                    standardCFMD(KSNAME, CFNAME, 1, UTF8Type.instance, Int32Type.instance, Int32Type.instance),
                                    standardCFMD(KSNAME, CFNAME_INDEXED, 1, UTF8Type.instance, Int32Type.instance, Int32Type.instance));
    }

    public RangeTombstoneTest(CompactionParams compactionParams)
    {
        Keyspace ks = Keyspace.open(KSNAME);
        ColumnFamilyStore cfs = ks.getColumnFamilyStore(CFNAME);
        SchemaTestUtil.announceTableUpdate(cfs.metadata().unbuild().compaction(compactionParams).build());
        cfs.disableAutoCompaction(); // don't trigger compaction at 4 sstables
        cfs = ks.getColumnFamilyStore(CFNAME_INDEXED);
        SchemaTestUtil.announceTableUpdate(cfs.metadata().unbuild().compaction(compactionParams).build());
        cfs.disableAutoCompaction(); // don't trigger compaction at 4 sstables
    }

    @Test
    public void simpleQueryWithRangeTombstoneTest() throws Exception
    {
        Keyspace keyspace = Keyspace.open(KSNAME);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CFNAME);
        boolean enforceStrictLiveness = cfs.metadata().enforceStrictLiveness();

        // Inserting data
        String key = "k1";

        UpdateBuilder builder;

        builder = UpdateBuilder.create(cfs.metadata(), key).withTimestamp(0);
        for (int i = 0; i < 40; i += 2)
            builder.newRow(i).add("val", i);
        builder.applyUnsafe();
        Util.flush(cfs);

        new RowUpdateBuilder(cfs.metadata(), 1, key).addRangeTombstone(10, 22).build().applyUnsafe();

        builder = UpdateBuilder.create(cfs.metadata(), key).withTimestamp(2);
        for (int i = 1; i < 40; i += 2)
            builder.newRow(i).add("val", i);
        builder.applyUnsafe();

        new RowUpdateBuilder(cfs.metadata(), 3, key).addRangeTombstone(19, 27).build().applyUnsafe();
        // We don't flush to test with both a range tomsbtone in memtable and in sstable

        // Queries by name
        int[] live = new int[]{ 4, 9, 11, 17, 28 };
        int[] dead = new int[]{ 12, 19, 21, 24, 27 };

        AbstractReadCommandBuilder.SinglePartitionBuilder cmdBuilder = Util.cmd(cfs, key);
        for (int i : live)
            cmdBuilder.includeRow(i);
        for (int i : dead)
            cmdBuilder.includeRow(i);

        Partition partition = Util.getOnlyPartitionUnfiltered(cmdBuilder.build());
        long nowInSec = FBUtilities.nowInSeconds();

        for (int i : live)
            assertTrue("Row " + i + " should be live",
                       partition.getRow(Clustering.make(bb(i))).hasLiveData(nowInSec, enforceStrictLiveness));
        for (int i : dead)
            assertFalse("Row " + i + " shouldn't be live",
                        partition.getRow(Clustering.make(bb(i))).hasLiveData(nowInSec, enforceStrictLiveness));

        // Queries by slices
        partition = Util.getOnlyPartitionUnfiltered(Util.cmd(cfs, key).fromIncl(7).toIncl(30).build());

        for (int i : new int[]{ 7, 8, 9, 11, 13, 15, 17, 28, 29, 30 })
            assertTrue("Row " + i + " should be live",
                       partition.getRow(Clustering.make(bb(i))).hasLiveData(nowInSec, enforceStrictLiveness));
        for (int i : new int[]{ 10, 12, 14, 16, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27 })
            assertFalse("Row " + i + " shouldn't be live",
                        partition.getRow(Clustering.make(bb(i))).hasLiveData(nowInSec, enforceStrictLiveness));
    }

    @Test
    public void rangeTombstoneFilteringTest() throws Exception
    {
        CompactionManager.instance.disableAutoCompaction();
        Keyspace keyspace = Keyspace.open(KSNAME);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CFNAME);

        // Inserting data
        String key = "k111";

        UpdateBuilder builder = UpdateBuilder.create(cfs.metadata(), key).withTimestamp(0);
        for (int i = 0; i < 40; i += 2)
            builder.newRow(i).add("val", i);
        builder.applyUnsafe();

        new RowUpdateBuilder(cfs.metadata(), 1, key).addRangeTombstone(5, 10).build().applyUnsafe();

        new RowUpdateBuilder(cfs.metadata(), 2, key).addRangeTombstone(15, 20).build().applyUnsafe();

        ImmutableBTreePartition partition;

        partition = Util.getOnlyPartitionUnfiltered(Util.cmd(cfs, key).fromIncl(11).toIncl(14).build());
        Collection<RangeTombstone> rt = rangeTombstones(partition);
        assertEquals(0, rt.size());

        partition = Util.getOnlyPartitionUnfiltered(Util.cmd(cfs, key).fromIncl(11).toIncl(15).build());
        rt = rangeTombstones(partition);
        assertEquals(1, rt.size());

        partition = Util.getOnlyPartitionUnfiltered(Util.cmd(cfs, key).fromIncl(20).toIncl(25).build());
        rt = rangeTombstones(partition);
        assertEquals(1, rt.size());

        partition = Util.getOnlyPartitionUnfiltered(Util.cmd(cfs, key).fromIncl(12).toIncl(25).build());
        rt = rangeTombstones(partition);
        assertEquals(1, rt.size());

        partition = Util.getOnlyPartitionUnfiltered(Util.cmd(cfs, key).fromIncl(25).toIncl(35).build());
        rt = rangeTombstones(partition);
        assertEquals(0, rt.size());

        partition = Util.getOnlyPartitionUnfiltered(Util.cmd(cfs, key).fromIncl(1).toIncl(40).build());
        rt = rangeTombstones(partition);
        assertEquals(2, rt.size());

        partition = Util.getOnlyPartitionUnfiltered(Util.cmd(cfs, key).fromIncl(7).toIncl(17).build());
        rt = rangeTombstones(partition);
        assertEquals(2, rt.size());

        partition = Util.getOnlyPartitionUnfiltered(Util.cmd(cfs, key).fromIncl(5).toIncl(20).build());
        rt = rangeTombstones(partition);
        assertEquals(2, rt.size());

        partition = Util.getOnlyPartitionUnfiltered(Util.cmd(cfs, key).fromIncl(5).toIncl(20).build());
        rt = rangeTombstones(partition);
        assertEquals(2, rt.size());

        partition = Util.getOnlyPartitionUnfiltered(Util.cmd(cfs, key).fromIncl(1).toIncl(2).build());
        rt = rangeTombstones(partition);
        assertEquals(0, rt.size());

        partition = Util.getOnlyPartitionUnfiltered(Util.cmd(cfs, key).fromIncl(1).toIncl(5).build());
        rt = rangeTombstones(partition);
        assertEquals(1, rt.size());

        partition = Util.getOnlyPartitionUnfiltered(Util.cmd(cfs, key).fromIncl(1).toIncl(10).build());
        rt = rangeTombstones(partition);
        assertEquals(1, rt.size());

        partition = Util.getOnlyPartitionUnfiltered(Util.cmd(cfs, key).fromIncl(5).toIncl(6).build());
        rt = rangeTombstones(partition);
        assertEquals(1, rt.size());

        partition = Util.getOnlyPartitionUnfiltered(Util.cmd(cfs, key).fromIncl(17).toIncl(20).build());
        rt = rangeTombstones(partition);
        assertEquals(1, rt.size());

        partition = Util.getOnlyPartitionUnfiltered(Util.cmd(cfs, key).fromIncl(17).toIncl(18).build());
        rt = rangeTombstones(partition);
        assertEquals(1, rt.size());

        Slices.Builder sb = new Slices.Builder(cfs.getComparator());
        sb.add(ClusteringBound.create(cfs.getComparator(), true, true, 1), ClusteringBound.create(cfs.getComparator(), false, true, 10));
        sb.add(ClusteringBound.create(cfs.getComparator(), true, true, 16), ClusteringBound.create(cfs.getComparator(), false, true, 20));

        partition = Util.getOnlyPartitionUnfiltered(SinglePartitionReadCommand.create(cfs.metadata(), FBUtilities.nowInSeconds(), Util.dk(key), sb.build()));
        rt = rangeTombstones(partition);
        assertEquals(2, rt.size());
    }

    private Collection<RangeTombstone> rangeTombstones(ImmutableBTreePartition partition)
    {
        List<RangeTombstone> tombstones = new ArrayList<>();
        Iterators.addAll(tombstones, partition.deletionInfo().rangeIterator(false));
        return tombstones;
    }

    @Test
    public void testTrackTimesPartitionTombstone() throws ExecutionException, InterruptedException
    {
        Keyspace ks = Keyspace.open(KSNAME);
        ColumnFamilyStore cfs = ks.getColumnFamilyStore(CFNAME);
        cfs.truncateBlocking();
        String key = "rt_times";

        long nowInSec = FBUtilities.nowInSeconds();
        new Mutation(PartitionUpdate.fullPartitionDelete(cfs.metadata(), Util.dk(key), 1000, nowInSec)).apply();
        Util.flush(cfs);

        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
        assertTimes(sstable.getSSTableMetadata(), 1000, 1000, nowInSec);
        cfs.forceMajorCompaction();
        sstable = cfs.getLiveSSTables().iterator().next();
        assertTimes(sstable.getSSTableMetadata(), 1000, 1000, nowInSec);
    }

    @Test
    public void testTrackTimesPartitionTombstoneWithData() throws ExecutionException, InterruptedException
    {
        Keyspace ks = Keyspace.open(KSNAME);
        ColumnFamilyStore cfs = ks.getColumnFamilyStore(CFNAME);
        cfs.truncateBlocking();
        String key = "rt_times";

        UpdateBuilder.create(cfs.metadata(), key).withTimestamp(999).newRow(5).add("val", 5).apply();

        key = "rt_times2";
        long nowInSec = FBUtilities.nowInSeconds();
        new Mutation(PartitionUpdate.fullPartitionDelete(cfs.metadata(), Util.dk(key), 1000, nowInSec)).apply();
        Util.flush(cfs);

        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
        assertTimes(sstable.getSSTableMetadata(), 999, 1000, Long.MAX_VALUE);
        cfs.forceMajorCompaction();
        sstable = cfs.getLiveSSTables().iterator().next();
        assertTimes(sstable.getSSTableMetadata(), 999, 1000, Long.MAX_VALUE);
    }

    @Test
    public void testTrackTimesRangeTombstone() throws ExecutionException, InterruptedException
    {
        Keyspace ks = Keyspace.open(KSNAME);
        ColumnFamilyStore cfs = ks.getColumnFamilyStore(CFNAME);
        cfs.truncateBlocking();
        String key = "rt_times";
        long nowInSec = FBUtilities.nowInSeconds();
        new RowUpdateBuilder(cfs.metadata(), nowInSec, 1000L, key).addRangeTombstone(1, 2).build().apply();
        Util.flush(cfs);

        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
        assertTimes(sstable.getSSTableMetadata(), 1000, 1000, nowInSec);
        cfs.forceMajorCompaction();
        sstable = cfs.getLiveSSTables().iterator().next();
        assertTimes(sstable.getSSTableMetadata(), 1000, 1000, nowInSec);
    }

    @Test
    public void testTrackTimesRangeTombstoneWithData() throws ExecutionException, InterruptedException
    {
        Keyspace ks = Keyspace.open(KSNAME);
        ColumnFamilyStore cfs = ks.getColumnFamilyStore(CFNAME);
        cfs.truncateBlocking();
        String key = "rt_times";

        UpdateBuilder.create(cfs.metadata(), key).withTimestamp(999).newRow(5).add("val", 5).apply();

        key = "rt_times2";
        long nowInSec = FBUtilities.nowInSeconds();
        new Mutation(PartitionUpdate.fullPartitionDelete(cfs.metadata(), Util.dk(key), 1000, nowInSec)).apply();
        Util.flush(cfs);

        Util.flush(cfs);
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
        assertTimes(sstable.getSSTableMetadata(), 999, 1000, Long.MAX_VALUE);
        cfs.forceMajorCompaction();
        sstable = cfs.getLiveSSTables().iterator().next();
        assertTimes(sstable.getSSTableMetadata(), 999, 1000, Long.MAX_VALUE);
    }

    private void assertTimes(StatsMetadata metadata, long min, long max, long localDeletionTime)
    {
        assertEquals(min, metadata.minTimestamp);
        assertEquals(max, metadata.maxTimestamp);
        assertEquals(localDeletionTime, metadata.maxLocalDeletionTime);
    }

    @Test
    public void test7810() throws ExecutionException, InterruptedException
    {
        Keyspace ks = Keyspace.open(KSNAME);
        ColumnFamilyStore cfs = ks.getColumnFamilyStore(CFNAME);
        SchemaTestUtil.announceTableUpdate(cfs.metadata().unbuild().gcGraceSeconds(2).build());
        cfs.truncateBlocking();

        String key = "7810";

        UpdateBuilder builder = UpdateBuilder.create(cfs.metadata(), key).withTimestamp(0);
        for (int i = 10; i < 20; i ++)
            builder.newRow(i).add("val", i);
        builder.apply();
        Util.flush(cfs);

        new RowUpdateBuilder(cfs.metadata(), 1, key).addRangeTombstone(10, 11).build().apply();
        Util.flush(cfs);

        Thread.sleep(3000);
        cfs.forceMajorCompaction();
        checkUnfilteredContains(cfs, key, 8);
    }


    @Test
    public void testDB4980_mid() throws ExecutionException, InterruptedException
    {
        testDB4980("4980_m", 9, 11, 12, 14, 10, 13);
    }

    @Test
    public void testDB4980_left() throws ExecutionException, InterruptedException
    {
        testDB4980("4980_l", 10, 13, 12, 14, 9, 11);
    }

    @Test
    public void testDB4980_right() throws ExecutionException, InterruptedException
    {
        testDB4980("4980_r", 9, 11, 10, 13, 12, 14);
    }

    public void testDB4980(String key, int start1, int end1, int start2, int end2, int start3, int end3) throws ExecutionException, InterruptedException
    {
        Keyspace ks = Keyspace.open(KSNAME);
        ColumnFamilyStore cfs = ks.getColumnFamilyStore(CFNAME);
        SchemaTestUtil.announceTableUpdate(cfs.metadata().unbuild().gcGraceSeconds(GC_GRACE).build());
        cfs.truncateBlocking();

        UpdateBuilder builder = UpdateBuilder.create(cfs.metadata(), key).withTimestamp(0);
        for (int i = 10; i < 20; i ++)
            builder.newRow(i).add("val", i);
        builder.apply();
        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);

        long localTime = FBUtilities.nowInSeconds();
        new RowUpdateBuilder(cfs.metadata(), localTime - (GC_GRACE + 100), 1, 0, key)
            .addRangeTombstone(start1, end1)
            .build()
            .apply();
        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);

        new RowUpdateBuilder(cfs.metadata(), localTime - (GC_GRACE + 30), 2, 0, key)
            .addRangeTombstone(start2, end2)
            .build()
            .apply();
        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);

        // This one should stay
        new RowUpdateBuilder(cfs.metadata(), localTime, 3, 0, key)
            .addRangeTombstone(start3, end3)
            .build()
            .apply();
        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);

        cfs.forceMajorCompaction();
        checkUnfilteredContains(cfs, key, 7);
    }

    private void checkUnfilteredContains(ColumnFamilyStore cfs, String key, int expected)
    {
        assertEquals(1, cfs.getLiveSSTables().size());
        DecoratedKey dkey = cfs.metadata().partitioner.decorateKey(((AbstractType<String>) cfs.metadata().partitionKeyType).decompose(key));
        UnfilteredRowIterator iter = cfs.getLiveSSTables().iterator().next().rowIterator(dkey,
                                                                                         Slices.ALL,
                                                                                         ColumnFilter.NONE,
                                                                                         false,
                                                                                         SSTableReadsListener.NOOP_LISTENER);
        iter = new OrderCheckingIterator(iter);
        assertEquals(expected, Iterators.size(iter));
    }

    @Test
    public void test7808_1() throws ExecutionException, InterruptedException
    {
        Keyspace ks = Keyspace.open(KSNAME);
        ColumnFamilyStore cfs = ks.getColumnFamilyStore(CFNAME);
        SchemaTestUtil.announceTableUpdate(cfs.metadata().unbuild().gcGraceSeconds(2).build());
        cfs.truncateBlocking();

        String key = "7808_1";
        UpdateBuilder builder = UpdateBuilder.create(cfs.metadata(), key).withTimestamp(0);
        for (int i = 0; i < 40; i += 2)
            builder.newRow(i).add("val", i);
        builder.apply();
        Util.flush(cfs);

        new Mutation(PartitionUpdate.fullPartitionDelete(cfs.metadata(), Util.dk(key), 1, 1)).apply();
        Util.flush(cfs);
        Thread.sleep(5);
        cfs.forceMajorCompaction();
    }

    @Test
    public void test7808_2() throws ExecutionException, InterruptedException
    {
        Keyspace ks = Keyspace.open(KSNAME);
        ColumnFamilyStore cfs = ks.getColumnFamilyStore(CFNAME);
        SchemaTestUtil.announceTableUpdate(cfs.metadata().unbuild().gcGraceSeconds(2).build());
        cfs.truncateBlocking();

        String key = "7808_2";
        UpdateBuilder builder = UpdateBuilder.create(cfs.metadata(), key).withTimestamp(0);
        for (int i = 10; i < 20; i ++)
            builder.newRow(i).add("val", i);
        builder.apply();
        Util.flush(cfs);

        new Mutation(PartitionUpdate.fullPartitionDelete(cfs.metadata(), Util.dk(key), 0, 0)).apply();

        UpdateBuilder.create(cfs.metadata(), key).withTimestamp(1).newRow(5).add("val", 5).apply();

        Util.flush(cfs);
        Thread.sleep(5);
        cfs.forceMajorCompaction();
        checkUnfilteredContains(cfs, key, 1);
    }

    @Test
    public void overlappingRangeTest() throws Exception
    {
        CompactionManager.instance.disableAutoCompaction();
        Keyspace keyspace = Keyspace.open(KSNAME);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CFNAME);
        boolean enforceStrictLiveness = cfs.metadata().enforceStrictLiveness();
        // Inserting data
        String key = "k2";

        UpdateBuilder builder = UpdateBuilder.create(cfs.metadata(), key).withTimestamp(0);
        for (int i = 0; i < 20; i++)
            builder.newRow(i).add("val", i);
        builder.applyUnsafe();
        Util.flush(cfs);

        new RowUpdateBuilder(cfs.metadata(), 1, key).addRangeTombstone(5, 15).build().applyUnsafe();
        Util.flush(cfs);

        new RowUpdateBuilder(cfs.metadata(), 1, key).addRangeTombstone(5, 10).build().applyUnsafe();
        Util.flush(cfs);

        new RowUpdateBuilder(cfs.metadata(), 2, key).addRangeTombstone(5, 8).build().applyUnsafe();
        Util.flush(cfs);

        Partition partition = Util.getOnlyPartitionUnfiltered(Util.cmd(cfs, key).build());
        long nowInSec = FBUtilities.nowInSeconds();

        for (int i = 0; i < 5; i++)
            assertTrue("Row " + i + " should be live",
                       partition.getRow(Clustering.make(bb(i))).hasLiveData(nowInSec, enforceStrictLiveness));
        for (int i = 16; i < 20; i++)
            assertTrue("Row " + i + " should be live",
                       partition.getRow(Clustering.make(bb(i))).hasLiveData(nowInSec, enforceStrictLiveness));
        for (int i = 5; i <= 15; i++)
            assertFalse("Row " + i + " shouldn't be live",
                        partition.getRow(Clustering.make(bb(i))).hasLiveData(nowInSec, enforceStrictLiveness));

        // Compact everything and re-test
        CompactionManager.instance.performMaximal(cfs, false);
        partition = Util.getOnlyPartitionUnfiltered(Util.cmd(cfs, key).build());

        for (int i = 0; i < 5; i++)
            assertTrue("Row " + i + " should be live",
                       partition.getRow(Clustering.make(bb(i))).hasLiveData(FBUtilities.nowInSeconds(),
                                                                            enforceStrictLiveness));
        for (int i = 16; i < 20; i++)
            assertTrue("Row " + i + " should be live",
                       partition.getRow(Clustering.make(bb(i))).hasLiveData(FBUtilities.nowInSeconds(),
                                                                            enforceStrictLiveness));
        for (int i = 5; i <= 15; i++)
            assertFalse("Row " + i + " shouldn't be live",
                        partition.getRow(Clustering.make(bb(i))).hasLiveData(nowInSec, enforceStrictLiveness));
    }

    @Test
    public void reverseQueryTest() throws Exception
    {
        Keyspace table = Keyspace.open(KSNAME);
        ColumnFamilyStore cfs = table.getColumnFamilyStore(CFNAME);

        // Inserting data
        String key = "k3";

        UpdateBuilder.create(cfs.metadata(), key).withTimestamp(0).newRow(2).add("val", 2).applyUnsafe();
        Util.flush(cfs);

        new RowUpdateBuilder(cfs.metadata(), 1, key).addRangeTombstone(0, 10).build().applyUnsafe();
        UpdateBuilder.create(cfs.metadata(), key).withTimestamp(2).newRow(1).add("val", 1).applyUnsafe();
        Util.flush(cfs);

        // Get the last value of the row
        FilteredPartition partition = Util.getOnlyPartition(Util.cmd(cfs, key).build());
        assertTrue(partition.rowCount() > 0);

        int last = i(partition.unfilteredIterator(ColumnFilter.all(cfs.metadata()), Slices.ALL, true).next().clustering().bufferAt(0));
        assertEquals("Last column should be column 1 since column 2 has been deleted", 1, last);
    }

    @Test
    public void testRowWithRangeTombstonesUpdatesSecondaryIndex() throws Exception
    {
        Keyspace table = Keyspace.open(KSNAME);
        ColumnFamilyStore cfs = table.getColumnFamilyStore(CFNAME_INDEXED);
        ByteBuffer key = ByteBufferUtil.bytes("k5");
        ByteBuffer indexedColumnName = ByteBufferUtil.bytes("val");

        cfs.truncateBlocking();
        cfs.disableAutoCompaction();

        ColumnMetadata cd = cfs.metadata().getColumn(indexedColumnName).copy();
        IndexMetadata indexDef =
            IndexMetadata.fromIndexTargets(
            Collections.singletonList(new IndexTarget(cd.name, IndexTarget.Type.VALUES)),
                                           "test_index",
                                           IndexMetadata.Kind.CUSTOM,
                                           ImmutableMap.of(IndexTarget.CUSTOM_INDEX_OPTION_NAME,
                                                           StubIndex.class.getName()));

        TableMetadata current = cfs.metadata();

        if (!current.indexes.get("test_index").isPresent())
        {
            TableMetadata updated =
                current.unbuild()
                       .indexes(current.indexes.with(indexDef))
                       .build();
            SchemaTestUtil.announceTableUpdate(updated);
        }

        Future<?> rebuild = cfs.indexManager.addIndex(indexDef, false);
        // If rebuild there is, wait for the rebuild to finish so it doesn't race with the following insertions
        if (rebuild != null)
            rebuild.get();

        StubIndex index = (StubIndex)cfs.indexManager.listIndexes()
                                                     .stream()
                                                     .filter(i -> "test_index".equals(i.getIndexMetadata().name))
                                                     .findFirst()
                                                     .orElseThrow(() -> new RuntimeException(new AssertionError("Index not found")));
        index.reset();

        UpdateBuilder builder = UpdateBuilder.create(cfs.metadata(), key).withTimestamp(0);
        for (int i = 0; i < 10; i++)
            builder.newRow(i).add("val", i);
        builder.applyUnsafe();
        Util.flush(cfs);

        new RowUpdateBuilder(cfs.metadata(), 0, key).addRangeTombstone(0, 7).build().applyUnsafe();
        Util.flush(cfs);

        assertEquals(10, index.rowsInserted.size());

        CompactionManager.instance.performMaximal(cfs, false);

        // compacted down to single sstable
        assertEquals(1, cfs.getLiveSSTables().size());

        assertEquals(8, index.rowsDeleted.size());
    }

    @Test
    public void testRangeTombstoneCompaction() throws Exception
    {
        Keyspace table = Keyspace.open(KSNAME);
        ColumnFamilyStore cfs = table.getColumnFamilyStore(CFNAME);
        ByteBuffer key = ByteBufferUtil.bytes("k4");

        // remove any existing sstables before starting
        cfs.truncateBlocking();
        cfs.disableAutoCompaction();

        UpdateBuilder builder = UpdateBuilder.create(cfs.metadata(), key).withTimestamp(0);
        for (int i = 0; i < 10; i += 2)
            builder.newRow(i).add("val", i);
        builder.applyUnsafe();
        Util.flush(cfs);

        new RowUpdateBuilder(cfs.metadata(), 0, key).addRangeTombstone(0, 7).build().applyUnsafe();
        Util.flush(cfs);

        // there should be 2 sstables
        assertEquals(2, cfs.getLiveSSTables().size());

        // compact down to single sstable
        CompactionManager.instance.performMaximal(cfs, false);
        assertEquals(1, cfs.getLiveSSTables().size());

        // test the physical structure of the sstable i.e. rt & columns on disk
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
        try (UnfilteredPartitionIterator scanner = sstable.getScanner())
        {
            try (UnfilteredRowIterator iter = scanner.next())
            {
                // after compaction, we should have a single RT with a single row (the row 8)
                Unfiltered u1 = iter.next();
                assertTrue("Expecting open marker, got " + u1.toString(cfs.metadata()), u1 instanceof RangeTombstoneMarker);
                Unfiltered u2 = iter.next();
                assertTrue("Expecting close marker, got " + u2.toString(cfs.metadata()), u2 instanceof RangeTombstoneMarker);
                Unfiltered u3 = iter.next();
                assertTrue("Expecting row, got " + u3.toString(cfs.metadata()), u3 instanceof Row);
            }
        }
    }

    @Test
    public void testOverwritesToDeletedColumns() throws Exception
    {
        Keyspace table = Keyspace.open(KSNAME);
        ColumnFamilyStore cfs = table.getColumnFamilyStore(CFNAME_INDEXED);
        ByteBuffer key = ByteBufferUtil.bytes("k6");
        ByteBuffer indexedColumnName = ByteBufferUtil.bytes("val");

        cfs.truncateBlocking();
        cfs.disableAutoCompaction();

        ColumnMetadata cd = cfs.metadata().getColumn(indexedColumnName).copy();
        IndexMetadata indexDef =
            IndexMetadata.fromIndexTargets(
            Collections.singletonList(new IndexTarget(cd.name, IndexTarget.Type.VALUES)),
                                           "test_index",
                                           IndexMetadata.Kind.CUSTOM,
                                           ImmutableMap.of(IndexTarget.CUSTOM_INDEX_OPTION_NAME,
                                                           StubIndex.class.getName()));

        TableMetadata current = cfs.metadata();

        if (!current.indexes.get("test_index").isPresent())
        {
            TableMetadata updated =
                current.unbuild()
                       .indexes(current.indexes.with(indexDef))
                       .build();
            SchemaTestUtil.announceTableUpdate(updated);
        }

        Future<?> rebuild = cfs.indexManager.addIndex(indexDef, false);
        // If rebuild there is, wait for the rebuild to finish so it doesn't race with the following insertions
        if (rebuild != null)
            rebuild.get();

        StubIndex index = (StubIndex)cfs.indexManager.getIndexByName("test_index");
        index.reset();

        UpdateBuilder.create(cfs.metadata(), key).withTimestamp(0).newRow(1).add("val", 1).applyUnsafe();

        // add a RT which hides the column we just inserted
        new RowUpdateBuilder(cfs.metadata(), 1, key).addRangeTombstone(0, 1).build().applyUnsafe();

        // now re-insert that column
        UpdateBuilder.create(cfs.metadata(), key).withTimestamp(2).newRow(1).add("val", 1).applyUnsafe();

        Util.flush(cfs);

        // We should have 1 insert and 1 update to the indexed "1" column
        // CASSANDRA-6640 changed index update to just update, not insert then delete
        assertEquals(1, index.rowsInserted.size());
        assertEquals(1, index.rowsUpdated.size());
    }

    private static ByteBuffer bb(int i)
    {
        return ByteBufferUtil.bytes(i);
    }

    private static int i(ByteBuffer bb)
    {
        return ByteBufferUtil.toInt(bb);
    }
}

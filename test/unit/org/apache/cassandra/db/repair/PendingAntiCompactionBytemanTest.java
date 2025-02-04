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

package org.apache.cassandra.db.repair;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.CompactionInterruptedException;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.RangesAtEndpoint;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.utils.TimeUUID;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.assertj.core.api.Assertions;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMRules;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;

import static org.apache.cassandra.db.ColumnFamilyStore.FlushReason.UNIT_TESTS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(BMUnitRunner.class)
public class PendingAntiCompactionBytemanTest extends AbstractPendingAntiCompactionTest
{
    @BMRules(rules = { @BMRule(name = "Throw exception anticompaction",
                               targetClass = "Range$OrderedRangeContainmentChecker",
                               targetMethod = "test",
                               action = "throw new org.apache.cassandra.db.compaction.CompactionInterruptedException" +
                                        "(\"antiCompactionExceptionTest\", org.apache.cassandra.db.compaction.TableOperation$StopTrigger.UNIT_TESTS);")} )
    @Test
    public void testExceptionAnticompaction()
    {
        cfs.disableAutoCompaction();
        cfs2.disableAutoCompaction();
        ExecutorService es = Executors.newFixedThreadPool(1);
        makeSSTables(4, cfs, 5);
        makeSSTables(4, cfs2, 5);
        List<Range<Token>> ranges = new ArrayList<>();

        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
            ranges.add(new Range<>(sstable.getFirst().getToken(), sstable.getLast().getToken()));
        }
        TimeUUID prsid = prepareSession();
        PendingAntiCompaction pac = new PendingAntiCompaction(prsid, Lists.newArrayList(cfs, cfs2), atEndpoint(ranges, NO_RANGES), es, () -> false);
        Assertions.assertThatThrownBy(() -> pac.run().get())
                  .hasCauseInstanceOf(CompactionInterruptedException.class)
                  .hasMessageContaining("Compaction interrupted due to unit tests");

        // Note that since we fail the PAC immediately when any of the anticompactions fail we need to wait for the other
        // AC to finish as well before asserting that we have nothing compacting.
        CompactionManager.instance.waitForCessation(Lists.newArrayList(cfs, cfs2), (sstable) -> true);
        // and make sure nothing is marked compacting
        assertTrue(cfs.getTracker().getCompacting().isEmpty());
        assertTrue(cfs2.getTracker().getCompacting().isEmpty());
        assertEquals(4, cfs.getLiveSSTables().size());
        assertEquals(4, cfs2.getLiveSSTables().size());
    }

    private static RangesAtEndpoint atEndpoint(Collection<Range<Token>> full, Collection<Range<Token>> trans)
    {
        RangesAtEndpoint.Builder builder = RangesAtEndpoint.builder(local);
        for (Range<Token> range : full)
            builder.add(new Replica(local, range, true));

        for (Range<Token> range : trans)
            builder.add(new Replica(local, range, false));

        return builder.build();
    }

    @BMRules(rules = { @BMRule(name = "Abort anti-compaction after first call to onOperationStart",
            targetClass = "CompactionManager",
            targetMethod = "antiCompactGroup",
            condition = "not flagged(\"done\")",
            targetLocation = "AFTER INVOKE compactionRateLimiterAcquire",
            action = "org.apache.cassandra.db.compaction.CompactionManager.instance.stopCompaction(\"ANTICOMPACTION\");") } )
    @Test
    public void testStopAntiCompaction()
    {
        Assert.assertSame(ByteOrderedPartitioner.class, DatabaseDescriptor.getPartitioner().getClass());
        cfs.disableAutoCompaction();

        // create 2 sstables, one that will be split, and another that will be moved
        for (int i = 0; i < 10; i++)
        {
            QueryProcessor.executeInternal(String.format("INSERT INTO %s.%s (k, v) VALUES (?, ?)", ks, tbl), i, i);
        }
        cfs.forceBlockingFlush(UNIT_TESTS);
        for (int i = 10; i < 20; i++)
        {
            QueryProcessor.executeInternal(String.format("INSERT INTO %s.%s (k, v) VALUES (?, ?)", ks, tbl), i, i);
        }
        cfs.forceBlockingFlush(UNIT_TESTS);

        assertEquals(2, cfs.getLiveSSTables().size());
        assertEquals(0, cfs.getLiveSSTables().stream().filter(SSTableReader::isPendingRepair).count());

        Token left = ByteOrderedPartitioner.instance.getToken(ByteBufferUtil.bytes(5));
        Token right = ByteOrderedPartitioner.instance.getToken(ByteBufferUtil.bytes(15));
        List<Range<Token>> ranges = Collections.singletonList(new Range<>(left, right));
        List<ColumnFamilyStore> tables = Collections.singletonList(cfs);

        // create a repair session so the anti-compaction job can find it
        TimeUUID sessionID = TimeUUID.Generator.nextTimeUUID();
        ActiveRepairService.instance().registerParentRepairSession(sessionID, InetAddressAndPort.getLocalHost(), tables, ranges, true, 1, true, PreviewKind.NONE);

        ExecutorService executor = Executors.newSingleThreadExecutor();
        try
        {
            PendingAntiCompaction pac = new PendingAntiCompaction(sessionID, tables, atEndpoint(ranges, NO_RANGES), executor, () -> false);
            Future<?> future = pac.run();
            Assertions.assertThatThrownBy(future::get)
                      .hasCauseInstanceOf(CompactionInterruptedException.class)
                      .hasMessageContaining("Compaction interrupted due to user request");
        }
        finally
        {
            executor.shutdown();
        }

        assertEquals(2, cfs.getLiveSSTables().size());
        assertEquals(0, cfs.getLiveSSTables().stream().filter(SSTableReader::isPendingRepair).count());
    }
}

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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.notifications.SSTableAddedNotification;
import org.apache.cassandra.notifications.SSTableDeletingNotification;
import org.apache.cassandra.notifications.SSTableListChangedNotification;
import org.apache.cassandra.notifications.SSTableRepairStatusChanged;
import org.apache.cassandra.repair.NoSuchRepairSessionException;
import org.apache.cassandra.repair.consistent.LocalSession;
import org.apache.cassandra.repair.consistent.LocalSessionAccessor;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.TimeUUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Tests CompactionStrategyContainer's handling of pending repair sstables
 */
public class CompactionStrategyManagerPendingRepairTest extends AbstractPendingRepairTest implements CompactionStrategyContainerPendingRepairTest
{
    @Override
    public String createTableCql()
    {
        // Note: This test is tightly coupled to the LegacyAbstractCompactionStrategy so cannot use the default UCS
        // UCS is tested in UnifiedCompactionContainerPendingRepairTest
        return String.format("CREATE TABLE %s.%s (k INT PRIMARY KEY, v INT) WITH COMPACTION={'class':'SizeTieredCompactionStrategy'}",
                             ks, tbl);
    }


    private boolean transientContains(SSTableReader sstable)
    {
        return ((CompactionStrategyManager) compactionStrategyContainer)
               .getTransientRepairsUnsafe()
               .containsSSTable(sstable);
    }

    private boolean pendingContains(SSTableReader sstable)
    {
        return ((CompactionStrategyManager) compactionStrategyContainer)
               .getPendingRepairsUnsafe()
               .containsSSTable(sstable);
    }

    private boolean repairedContains(SSTableReader sstable)
    {
        return ((CompactionStrategyManager) compactionStrategyContainer)
               .getRepairedUnsafe()
               .containsSSTable(sstable);
    }

    private boolean unrepairedContains(SSTableReader sstable)
    {
        return ((CompactionStrategyManager) compactionStrategyContainer)
               .getUnrepairedUnsafe()
               .containsSSTable(sstable);
    }

    private boolean hasPendingStrategiesFor(TimeUUID sessionID)
    {
        return !Iterables.isEmpty(((CompactionStrategyManager) compactionStrategyContainer)
                                  .getPendingRepairsUnsafe()
                                  .getStrategiesFor(sessionID));
    }

    private boolean hasTransientStrategiesFor(TimeUUID sessionID)
    {
        return !Iterables.isEmpty(((CompactionStrategyManager) compactionStrategyContainer)
                                  .getTransientRepairsUnsafe()
                                  .getStrategiesFor(sessionID));
    }

    private void assertCompactionStrategyManagerPendingRepairs(boolean expectedEmpty)
    {
        assertEquals(expectedEmpty, ((CompactionStrategyManager) cfs.getCompactionStrategy()).pendingRepairs().isEmpty());
    }

    /**
     * Pending repair strategy should be created when we encounter a new pending id
     */
    @Override
    @Test
    public void testSstableAdded() throws IOException
    {
        TimeUUID repairID = registerSession(cfs, true, true);
        LocalSessionAccessor.prepareUnsafe(repairID, COORDINATOR, PARTICIPANTS);
        Assert.assertTrue(Iterables.isEmpty(((CompactionStrategyManager) compactionStrategyContainer)
                                            .getPendingRepairsUnsafe()
                                            .allStrategies()));

        SSTableReader sstable = makeSSTable(true);
        Assert.assertFalse(sstable.isRepaired());
        Assert.assertFalse(sstable.isPendingRepair());

        mutateRepaired(sstable, repairID, false);
        Assert.assertFalse(sstable.isRepaired());
        Assert.assertTrue(sstable.isPendingRepair());
        Assert.assertFalse(hasPendingStrategiesFor(repairID));
        Assert.assertFalse(hasTransientStrategiesFor(repairID));

        // add the sstable
        compactionStrategyContainer.handleNotification(new SSTableAddedNotification(Collections.singleton(sstable), null), cfs.getTracker());
        Assert.assertFalse(repairedContains(sstable));
        Assert.assertFalse(unrepairedContains(sstable));
        Assert.assertTrue(pendingContains(sstable));
        Assert.assertTrue(hasPendingStrategiesFor(repairID));
        Assert.assertFalse(hasTransientStrategiesFor(repairID));
    }

    @Override
    @Test
    public void testSstableListChangedAddAndRemove() throws IOException
    {
        TimeUUID repairID = registerSession(cfs, true, true);
        LocalSessionAccessor.prepareUnsafe(repairID, COORDINATOR, PARTICIPANTS);

        SSTableReader sstable1 = makeSSTable(true);
        mutateRepaired(sstable1, repairID, false);

        SSTableReader sstable2 = makeSSTable(true);
        mutateRepaired(sstable2, repairID, false);

        Assert.assertFalse(repairedContains(sstable1));
        Assert.assertFalse(unrepairedContains(sstable1));
        Assert.assertFalse(repairedContains(sstable2));
        Assert.assertFalse(unrepairedContains(sstable2));
        Assert.assertFalse(hasPendingStrategiesFor(repairID));
        Assert.assertFalse(hasTransientStrategiesFor(repairID));

        // add only
        SSTableListChangedNotification notification;
        notification = new SSTableListChangedNotification(Collections.singleton(sstable1),
                                                          Collections.emptyList(),
                                                          OperationType.COMPACTION,
                                                          Optional.empty());
        compactionStrategyContainer.handleNotification(notification, cfs.getTracker());

        Assert.assertFalse(repairedContains(sstable1));
        Assert.assertFalse(unrepairedContains(sstable1));
        Assert.assertTrue(pendingContains(sstable1));
        Assert.assertFalse(repairedContains(sstable2));
        Assert.assertFalse(unrepairedContains(sstable2));
        Assert.assertFalse(pendingContains(sstable2));
        Assert.assertTrue(hasPendingStrategiesFor(repairID));
        Assert.assertFalse(hasTransientStrategiesFor(repairID));

        // remove and add
        notification = new SSTableListChangedNotification(Collections.singleton(sstable2),
                                                          Collections.singleton(sstable1),
                                                          OperationType.COMPACTION,
                                                          Optional.empty());
        compactionStrategyContainer.handleNotification(notification, cfs.getTracker());

        Assert.assertFalse(repairedContains(sstable1));
        Assert.assertFalse(unrepairedContains(sstable1));
        Assert.assertFalse(pendingContains(sstable1));
        Assert.assertFalse(repairedContains(sstable2));
        Assert.assertFalse(unrepairedContains(sstable2));
        Assert.assertTrue(pendingContains(sstable2));
    }

    @Override
    @Test
    public void testSstableRepairStatusChanged() throws IOException
    {
        TimeUUID repairID = registerSession(cfs, true, true);
        LocalSessionAccessor.prepareUnsafe(repairID, COORDINATOR, PARTICIPANTS);

        // add as unrepaired
        SSTableReader sstable = makeSSTable(false);
        Assert.assertTrue(unrepairedContains(sstable));
        Assert.assertFalse(repairedContains(sstable));
        Assert.assertFalse(hasPendingStrategiesFor(repairID));
        Assert.assertFalse(hasTransientStrategiesFor(repairID));

        SSTableRepairStatusChanged notification;

        // change to pending repaired
        mutateRepaired(sstable, repairID, false);
        notification = new SSTableRepairStatusChanged(Collections.singleton(sstable));
        compactionStrategyContainer.handleNotification(notification, cfs.getTracker());
        Assert.assertFalse(unrepairedContains(sstable));
        Assert.assertFalse(repairedContains(sstable));
        Assert.assertTrue(hasPendingStrategiesFor(repairID));
        Assert.assertFalse(hasTransientStrategiesFor(repairID));
        Assert.assertTrue(pendingContains(sstable));

        // change to repaired
        mutateRepaired(sstable, System.currentTimeMillis());
        notification = new SSTableRepairStatusChanged(Collections.singleton(sstable));
        compactionStrategyContainer.handleNotification(notification, cfs.getTracker());
        Assert.assertFalse(unrepairedContains(sstable));
        Assert.assertTrue(repairedContains(sstable));
        Assert.assertFalse(pendingContains(sstable));
    }

    @Override
    @Test
    public void testSstableDeleted() throws IOException
    {
        TimeUUID repairID = registerSession(cfs, true, true);
        LocalSessionAccessor.prepareUnsafe(repairID, COORDINATOR, PARTICIPANTS);

        SSTableReader sstable = makeSSTable(true);
        mutateRepaired(sstable, repairID, false);
        compactionStrategyContainer.handleNotification(new SSTableAddedNotification(Collections.singleton(sstable), null), cfs.getTracker());
        Assert.assertTrue(pendingContains(sstable));

        // delete sstable
        SSTableDeletingNotification notification = new SSTableDeletingNotification(sstable);
        compactionStrategyContainer.handleNotification(notification, cfs.getTracker());
        Assert.assertFalse(pendingContains(sstable));
        Assert.assertFalse(unrepairedContains(sstable));
        Assert.assertFalse(repairedContains(sstable));
    }

    /**
     * CompactionStrategyManager.getStrategies should include
     * pending repair strategies when appropriate
     */
    @Override
    @Test
    public void testStrategiesContainsPendingRepair() throws IOException
    {
        TimeUUID repairID = registerSession(cfs, true, true);
        LocalSessionAccessor.prepareUnsafe(repairID, COORDINATOR, PARTICIPANTS);

        Assert.assertTrue(compactionStrategyContainer.getStrategies(false, repairID).isEmpty());

        SSTableReader sstable = makeSSTable(true);
        mutateRepaired(sstable, repairID, false);
        compactionStrategyContainer.handleNotification(new SSTableAddedNotification(Collections.singleton(sstable), null), cfs.getTracker());

        Assert.assertFalse(compactionStrategyContainer.getStrategies(false, repairID).isEmpty());
    }

    /**
     * Tests that finalized repairs result in cleanup compaction tasks
     * which reclassify the sstables as repaired
     */
    @Override
    @Test
    public void testCleanupCompactionFinalized() throws NoSuchRepairSessionException
    {
        TimeUUID repairID = registerSession(cfs, true, true);
        LocalSessionAccessor.prepareUnsafe(repairID, COORDINATOR, PARTICIPANTS);
        SSTableReader sstable = makeSSTable(true);
        mutateRepaired(sstable, repairID, false);
        compactionStrategyContainer.handleNotification(new SSTableAddedNotification(Collections.singleton(sstable), null), cfs.getTracker());
        LocalSessionAccessor.finalizeUnsafe(repairID);
        Assert.assertTrue(hasPendingStrategiesFor(repairID));
        Assert.assertFalse(hasTransientStrategiesFor(repairID));
        Assert.assertTrue(pendingContains(sstable));
        Assert.assertTrue(sstable.isPendingRepair());
        Assert.assertFalse(sstable.isRepaired());

        cfs.getCompactionStrategyContainer().enable(); // enable compaction to fetch next background task
        Collection<AbstractCompactionTask> compactionTasks = compactionStrategyContainer.getNextBackgroundTasks(FBUtilities.nowInSeconds());
        assertEquals(1, compactionTasks.size());
        AbstractCompactionTask compactionTask = compactionTasks.iterator().next();
        Assert.assertNotNull(compactionTask);
        Assert.assertSame(RepairFinishedCompactionTask.class, compactionTask.getClass());

        // run the compaction
        compactionTask.execute();

        Assert.assertTrue(repairedContains(sstable));
        Assert.assertFalse(unrepairedContains(sstable));
        Assert.assertFalse(pendingContains(sstable));
        Assert.assertFalse(hasPendingStrategiesFor(repairID));
        Assert.assertFalse(hasTransientStrategiesFor(repairID));

        // sstable should have pendingRepair cleared, and repairedAt set correctly
        long expectedRepairedAt = ActiveRepairService.instance().getParentRepairSession(repairID).repairedAt;
        Assert.assertFalse(sstable.isPendingRepair());
        Assert.assertTrue(sstable.isRepaired());
        assertEquals(expectedRepairedAt, sstable.getSSTableMetadata().repairedAt);
    }

    /**
     * Tests that finalized repairs racing with compactions on the same set of sstables don't leave unrepaired sstables behind
     *
     * This test checks that when a repair has been finalized but there are still pending sstables a finalize repair
     * compaction task is issued for that repair session.
     */
    @Test
    public void testFinalizedAndCompactionRace() throws IOException, NoSuchRepairSessionException
    {
        TimeUUID repairID = registerSession(cfs, true, true);
        LocalSessionAccessor.prepareUnsafe(repairID, COORDINATOR, PARTICIPANTS);

        int numberOfSStables = 4;
        List<SSTableReader> sstables = new ArrayList<>(numberOfSStables);
        for (int i = 0; i < numberOfSStables; i++)
        {
            SSTableReader sstable = makeSSTable(true);
            sstables.add(sstable);
            Assert.assertFalse(sstable.isRepaired());
            Assert.assertFalse(sstable.isPendingRepair());
        }

        // change to pending repair
        cfs.mutateRepaired(sstables, 0, repairID, false);
        compactionStrategyContainer.handleNotification(new SSTableAddedNotification(sstables, null), cfs.getTracker());
        for (SSTableReader sstable : sstables)
        {
            Assert.assertFalse(sstable.isRepaired());
            Assert.assertTrue(sstable.isPendingRepair());
            assertEquals(repairID, sstable.getPendingRepair());
        }

        // Get a compaction taks based on the sstables marked as pending repair
        compactionStrategyContainer.enable();
        assertEquals(numberOfSStables, cfs.getPendingRepairSSTables(repairID).size());
        Collection<AbstractCompactionTask> compactionTasks = compactionStrategyContainer.getNextBackgroundTasks(FBUtilities.nowInSeconds());
        assertEquals(1, compactionTasks.size());
        AbstractCompactionTask compactionTask = compactionTasks.iterator().next();
        assertNotNull(compactionTask);

        // Finalize the repair session
        LocalSessionAccessor.finalizeUnsafe(repairID);
        LocalSession session = ARS.consistent.local.getSession(repairID);
        ARS.consistent.local.sessionCompleted(session);
        Assert.assertTrue(hasPendingStrategiesFor(repairID));

        // run the compaction
        compactionTask.execute();

        // The repair session is finalized but there is an sstable left behind pending repair!
        SSTableReader compactedSSTable = cfs.getPendingRepairSSTables(repairID).iterator().next();
        assertEquals(repairID, compactedSSTable.getPendingRepair());
        assertEquals(1, cfs.getLiveSSTables().size());
        assertEquals(1, cfs.getPendingRepairSSTables(repairID).size());

        System.out.println("*********************************************************************************************");
        System.out.println(compactedSSTable);
        System.out.println("Pending repair UUID: " + compactedSSTable.getPendingRepair());
        System.out.println("Repaired at: " + compactedSSTable.getRepairedAt());
        System.out.println("Creation time: " + compactedSSTable.getCreationTimeFor(SSTableFormat.Components.DATA));
        System.out.println("Live sstables: " + cfs.getLiveSSTables().size());
        System.out.println("Pending repair sstables: " + cfs.getPendingRepairSSTables(repairID).size());
        System.out.println("*********************************************************************************************");

        // Run compaction again. It should pick up the pending repair sstable
        compactionTasks = compactionStrategyContainer.getNextBackgroundTasks(FBUtilities.nowInSeconds());
        if (!compactionTasks.isEmpty())
        {
            assertEquals(1, compactionTasks.size());
            compactionTask = compactionTasks.iterator().next();
            assertNotNull(compactionTask);
            Assert.assertSame(RepairFinishedCompactionTask.class, compactionTask.getClass());
            compactionTask.execute();
        }

        System.out.println("*********************************************************************************************");
        System.out.println(compactedSSTable);
        System.out.println("Pending repair UUID: " + compactedSSTable.getPendingRepair());
        System.out.println("Repaired at: " + compactedSSTable.getRepairedAt());
        System.out.println("Creation time: " + compactedSSTable.getCreationTimeFor(SSTableFormat.Components.DATA));
        System.out.println("Live sstables: " + cfs.getLiveSSTables().size());
        System.out.println("Pending repair sstables: " + cfs.getPendingRepairSSTables(repairID).size());
        System.out.println("*********************************************************************************************");

        assertEquals(0, cfs.getPendingRepairSSTables(repairID).size());
        assertEquals(1, cfs.getLiveSSTables().size());
        Assert.assertFalse(hasPendingStrategiesFor(repairID));
        Assert.assertFalse(hasTransientStrategiesFor(repairID));
        Assert.assertTrue(repairedContains(compactedSSTable));
        Assert.assertFalse(unrepairedContains(compactedSSTable));
        Assert.assertFalse(pendingContains(compactedSSTable));
        // sstable should have pendingRepair cleared, and repairedAt set correctly
        long expectedRepairedAt = ActiveRepairService.instance().getParentRepairSession(repairID).repairedAt;
        Assert.assertFalse(compactedSSTable.isPendingRepair());
        Assert.assertTrue(compactedSSTable.isRepaired());
        assertEquals(expectedRepairedAt, compactedSSTable.getSSTableMetadata().repairedAt);
    }

    /**
     * Tests that failed repairs result in cleanup compaction tasks
     * which reclassify the sstables as unrepaired
     */
    @Override
    @Test
    public void testCleanupCompactionFailed() throws IOException
    {
        TimeUUID repairID = registerSession(cfs, true, true);
        LocalSessionAccessor.prepareUnsafe(repairID, COORDINATOR, PARTICIPANTS);
        SSTableReader sstable = makeSSTable(true);
        mutateRepaired(sstable, repairID, false);
        compactionStrategyContainer.handleNotification(new SSTableAddedNotification(Collections.singleton(sstable), null), cfs.getTracker());
        LocalSessionAccessor.failUnsafe(repairID);

        Assert.assertTrue(hasPendingStrategiesFor(repairID));
        Assert.assertFalse(hasTransientStrategiesFor(repairID));
        Assert.assertTrue(pendingContains(sstable));
        Assert.assertTrue(sstable.isPendingRepair());
        Assert.assertFalse(sstable.isRepaired());

        cfs.getCompactionStrategyContainer().enable(); // enable compaction to fetch next background task
        Collection<AbstractCompactionTask> compactionTasks = compactionStrategyContainer.getNextBackgroundTasks(FBUtilities.nowInSeconds());
        assertEquals(1, compactionTasks.size());
        AbstractCompactionTask compactionTask = compactionTasks.iterator().next();
        Assert.assertNotNull(compactionTask);
        Assert.assertSame(RepairFinishedCompactionTask.class, compactionTask.getClass());

        // run the compaction
        compactionTask.execute();

        Assert.assertFalse(repairedContains(sstable));
        Assert.assertTrue(unrepairedContains(sstable));
        Assert.assertFalse(hasPendingStrategiesFor(repairID));
        Assert.assertFalse(hasTransientStrategiesFor(repairID));

        // sstable should have pendingRepair cleared, and repairedAt set correctly
        Assert.assertFalse(sstable.isPendingRepair());
        Assert.assertFalse(sstable.isRepaired());
        assertEquals(ActiveRepairService.UNREPAIRED_SSTABLE, sstable.getSSTableMetadata().repairedAt);
    }

    @Override
    @Test
    public void testSessionCompleted() throws IOException
    {
        TimeUUID repairID = registerSession(cfs, true, true);
        LocalSessionAccessor.prepareUnsafe(repairID, COORDINATOR, PARTICIPANTS);
        assertCompactionStrategyManagerPendingRepairs(true);

        // add sstable as unrepaired
        final boolean isOrphan = false;
        SSTableReader sstable = makeSSTable(isOrphan);

        // change to pending repair
        mutateRepaired(sstable, repairID, false);
        SSTableRepairStatusChanged notification = new SSTableRepairStatusChanged(Collections.singleton(sstable));
        compactionStrategyContainer.handleNotification(notification, cfs.getTracker());
        Assert.assertFalse(unrepairedContains(sstable));
        Assert.assertFalse(repairedContains(sstable));
        Assert.assertTrue(hasPendingStrategiesFor(repairID));
        Assert.assertFalse(hasTransientStrategiesFor(repairID));
        Assert.assertTrue(pendingContains(sstable));

        // finalize
        LocalSessionAccessor.finalizeUnsafe(repairID);

        // complete session
        ARS.consistent.local.sessionCompleted(ARS.consistent.local.getSession(repairID));

        // sstable is repaired
        Assert.assertFalse(unrepairedContains(sstable));
        Assert.assertTrue(repairedContains(sstable));
        Assert.assertFalse(pendingContains(sstable));
    }

    @Override
    @Test
    public void testSessionCompletedWithDifferentSSTables() throws IOException
    {
        TimeUUID repairID1 = registerSession(cfs, true, true);
        TimeUUID repairID2 = registerSession(cfs, true, true);
        LocalSessionAccessor.prepareUnsafe(repairID1, COORDINATOR, PARTICIPANTS);
        LocalSessionAccessor.prepareUnsafe(repairID2, COORDINATOR, PARTICIPANTS);
        assertCompactionStrategyManagerPendingRepairs(true);

        // add sstables as unrepaired
        final boolean isOrphan = false;
        SSTableReader sstable1 = makeSSTable(isOrphan);
        Assert.assertTrue(unrepairedContains(sstable1));

        SSTableReader sstable2 = makeSSTable(isOrphan);
        Assert.assertTrue(unrepairedContains(sstable2));

        SSTableReader sstable3 = makeSSTable(isOrphan);
        Assert.assertTrue(unrepairedContains(sstable3));

        // change sstable1 to pending repair for session 1
        mutateRepaired(sstable1, repairID1, false);
        SSTableRepairStatusChanged notification = new SSTableRepairStatusChanged(ImmutableList.of(sstable1));
        compactionStrategyContainer.handleNotification(notification, cfs.getTracker());
        Assert.assertFalse(sstable1.isRepaired());
        Assert.assertTrue(sstable1.isPendingRepair());
        Assert.assertTrue(hasPendingStrategiesFor(repairID1));
        Assert.assertFalse(hasTransientStrategiesFor(repairID1));

        // change sstable2 to pending repair for session 2
        mutateRepaired(sstable2, repairID2, false);
        notification = new SSTableRepairStatusChanged(ImmutableList.of(sstable2));
        compactionStrategyContainer.handleNotification(notification, cfs.getTracker());
        Assert.assertFalse(sstable2.isRepaired());
        Assert.assertTrue(sstable2.isPendingRepair());
        Assert.assertTrue(hasPendingStrategiesFor(repairID2));
        Assert.assertFalse(hasTransientStrategiesFor(repairID2));

        // change sstable3 to repaired
        mutateRepaired(sstable3, System.currentTimeMillis());
        Assert.assertTrue(sstable3.isRepaired());
        Assert.assertFalse(sstable3.isPendingRepair());

        // finalize session 1
        LocalSessionAccessor.finalizeUnsafe(repairID1);

        // simulate compaction on repaired sstable3
        cfs.getTracker().tryModify(sstable3, OperationType.COMPACTION);

        // completing session 1 will not require to disable compactions because:
        // * sstable2 belongs to a different session
        // * sstable3 is repaired
        ARS.consistent.local.sessionCompleted(ARS.consistent.local.getSession(repairID1));

        // now sstable1 and sstable3 are repaired
        Assert.assertTrue(sstable1.isRepaired());
        Assert.assertTrue(sstable3.isRepaired());
        Assert.assertTrue(sstable2.isPendingRepair());

        assertEquals(Collections.singleton(repairID2),
                     ((CompactionStrategyManager) compactionStrategyContainer).pendingRepairs());
    }

    @Override
    @Test
    public void testFinalizedSessionTransientCleanup() throws IOException
    {
        Assert.assertTrue(cfs.getLiveSSTables().isEmpty());
        TimeUUID repairID = registerSession(cfs, true, true);
        LocalSessionAccessor.prepareUnsafe(repairID, COORDINATOR, PARTICIPANTS);
        SSTableReader sstable = makeSSTable(true);
        mutateRepaired(sstable, repairID, true);
        compactionStrategyContainer.handleNotification(new SSTableAddedNotification(Collections.singleton(sstable), null), cfs.getTracker());
        LocalSessionAccessor.finalizeUnsafe(repairID);

        Assert.assertFalse(hasPendingStrategiesFor(repairID));
        Assert.assertTrue(hasTransientStrategiesFor(repairID));
        Assert.assertTrue(transientContains(sstable));
        Assert.assertFalse(pendingContains(sstable));
        Assert.assertFalse(repairedContains(sstable));
        Assert.assertFalse(unrepairedContains(sstable));

        cfs.getCompactionStrategyContainer().enable(); // enable compaction to fetch next background task
        Collection<AbstractCompactionTask> compactionTasks = compactionStrategyContainer.getNextBackgroundTasks(FBUtilities.nowInSeconds());
        assertEquals(1, compactionTasks.size());
        AbstractCompactionTask compactionTask = compactionTasks.iterator().next();
        Assert.assertNotNull(compactionTask);
        Assert.assertSame(RepairFinishedCompactionTask.class, compactionTask.getClass());

        // run the compaction
        compactionTask.execute();

        Assert.assertTrue(cfs.getLiveSSTables().isEmpty());
        Assert.assertFalse(hasPendingStrategiesFor(repairID));
        Assert.assertFalse(hasTransientStrategiesFor(repairID));
    }

    @Override
    @Test
    public void testFailedSessionTransientCleanup() throws IOException
    {
        Assert.assertTrue(cfs.getLiveSSTables().isEmpty());
        TimeUUID repairID = registerSession(cfs, true, true);
        LocalSessionAccessor.prepareUnsafe(repairID, COORDINATOR, PARTICIPANTS);
        SSTableReader sstable = makeSSTable(true);
        mutateRepaired(sstable, repairID, true);
        compactionStrategyContainer.handleNotification(new SSTableAddedNotification(Collections.singleton(sstable), null), cfs.getTracker());
        LocalSessionAccessor.failUnsafe(repairID);

        Assert.assertFalse(hasPendingStrategiesFor(repairID));
        Assert.assertTrue(hasTransientStrategiesFor(repairID));
        Assert.assertTrue(transientContains(sstable));
        Assert.assertFalse(pendingContains(sstable));
        Assert.assertFalse(repairedContains(sstable));
        Assert.assertFalse(unrepairedContains(sstable));

        cfs.getCompactionStrategyContainer().enable(); // enable compaction to fetch next background task
        Collection<AbstractCompactionTask> compactionTasks = compactionStrategyContainer.getNextBackgroundTasks(FBUtilities.nowInSeconds());
        assertEquals(1, compactionTasks.size());
        AbstractCompactionTask compactionTask = compactionTasks.iterator().next();
        Assert.assertNotNull(compactionTask);
        Assert.assertSame(RepairFinishedCompactionTask.class, compactionTask.getClass());

        // run the compaction
        compactionTask.execute();

        Assert.assertFalse(cfs.getLiveSSTables().isEmpty());
        Assert.assertFalse(hasPendingStrategiesFor(repairID));
        Assert.assertFalse(hasTransientStrategiesFor(repairID));
        Assert.assertFalse(transientContains(sstable));
        Assert.assertFalse(pendingContains(sstable));
        Assert.assertFalse(repairedContains(sstable));
        Assert.assertTrue(unrepairedContains(sstable));
    }
}

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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.TimeUUID;

/**
 * This class manages the sstables marked pending repair so that they can be assigned to legacy compaction
 * strategies via the legacy strategy container or manager.
 *
 * SSTables are classified as pending repair by the anti-compaction performed at the beginning
 * of an incremental repair, or when they're streamed in with a pending repair id. This prevents
 * unrepaired / pending repaired sstables from being compacted together. Once the repair session
 * has completed, or failed, sstables will be re-classified as part of the compaction process.
 */
class PendingRepairManager
{
    private static final Logger logger = LoggerFactory.getLogger(PendingRepairManager.class);

    private final CompactionRealm realm;
    private final CompactionStrategyFactory strategyFactory;
    private final CompactionParams params;
    private final boolean isTransient;
    private volatile ImmutableMap<TimeUUID, LegacyAbstractCompactionStrategy> strategies = ImmutableMap.of();

    /**
     * Indicates we're being asked to do something with an sstable that isn't marked pending repair
     */
    public static class IllegalSSTableArgumentException extends IllegalArgumentException
    {
        public IllegalSSTableArgumentException(String s)
        {
            super(s);
        }
    }

    PendingRepairManager(CompactionRealm realm, CompactionStrategyFactory strategyFactory, CompactionParams params, boolean isTransient)
    {
        this.realm = realm;
        this.strategyFactory = strategyFactory;
        this.params = params;
        this.isTransient = isTransient;
    }

    private ImmutableMap.Builder<TimeUUID, LegacyAbstractCompactionStrategy> mapBuilder()
    {
        return ImmutableMap.builder();
    }

    LegacyAbstractCompactionStrategy get(TimeUUID id)
    {
        return strategies.get(id);
    }

    LegacyAbstractCompactionStrategy get(CompactionSSTable sstable)
    {
        assert sstable.isPendingRepair();
        return get(sstable.getPendingRepair());
    }

    LegacyAbstractCompactionStrategy getOrCreate(TimeUUID id)
    {
        checkPendingID(id);
        assert id != null;
        LegacyAbstractCompactionStrategy strategy = get(id);
        if (strategy == null)
        {
            synchronized (this)
            {
                strategy = get(id);

                if (strategy == null)
                {
                    logger.debug("Creating {}.{} compaction strategy for pending repair: {}", realm.getKeyspaceName(), realm.getTableName(), id);
                    strategy = strategyFactory.createLegacyStrategy(params);
                    strategies = mapBuilder().putAll(strategies).put(id, strategy).build();
                }
            }
        }
        return strategy;
    }

    private static void checkPendingID(TimeUUID pendingID)
    {
        if (pendingID == null)
        {
            throw new IllegalSSTableArgumentException("sstable is not pending repair");
        }
    }

    LegacyAbstractCompactionStrategy getOrCreate(CompactionSSTable sstable)
    {
        return getOrCreate(sstable.getPendingRepair());
    }

    synchronized void removeSessionIfEmpty(TimeUUID sessionID)
    {
        if (!strategies.containsKey(sessionID) || !strategies.get(sessionID).getSSTables().isEmpty())
            return;

        logger.debug("Removing compaction strategy for pending repair {} on  {}.{}", sessionID, realm.getKeyspaceName(), realm.getTableName());
        strategies = ImmutableMap.copyOf(Maps.filterKeys(strategies, k -> !k.equals(sessionID)));
    }

    synchronized void removeSSTable(CompactionSSTable sstable)
    {
        for (Map.Entry<TimeUUID, LegacyAbstractCompactionStrategy> entry : strategies.entrySet())
        {
            entry.getValue().removeSSTable(sstable);
            removeSessionIfEmpty(entry.getKey());
        }
    }

    void removeSSTables(Iterable<CompactionSSTable> removed)
    {
        for (CompactionSSTable sstable : removed)
            removeSSTable(sstable);
    }

    synchronized void addSSTable(CompactionSSTable sstable)
    {
        Preconditions.checkArgument(sstable.isTransient() == isTransient);
        getOrCreate(sstable).addSSTable(sstable);
    }

    void addSSTables(Iterable<? extends CompactionSSTable> added)
    {
        for (CompactionSSTable sstable : added)
            addSSTable(sstable);
    }

    synchronized void replaceSSTables(Set<CompactionSSTable> removed, Set<CompactionSSTable> added)
    {
        if (removed.isEmpty() && added.isEmpty())
            return;

        // left=removed, right=added
        Map<TimeUUID, Pair<Set<CompactionSSTable>, Set<CompactionSSTable>>> groups = new HashMap<>();
        for (CompactionSSTable sstable : removed)
        {
            TimeUUID sessionID = sstable.getPendingRepair();
            if (!groups.containsKey(sessionID))
            {
                groups.put(sessionID, Pair.create(new HashSet<>(), new HashSet<>()));
            }
            groups.get(sessionID).left.add(sstable);
        }

        for (CompactionSSTable sstable : added)
        {
            TimeUUID sessionID = sstable.getPendingRepair();
            if (!groups.containsKey(sessionID))
            {
                groups.put(sessionID, Pair.create(new HashSet<>(), new HashSet<>()));
            }
            groups.get(sessionID).right.add(sstable);
        }

        for (Map.Entry<TimeUUID, Pair<Set<CompactionSSTable>, Set<CompactionSSTable>>> entry : groups.entrySet())
        {
            LegacyAbstractCompactionStrategy strategy = getOrCreate(entry.getKey());
            Set<CompactionSSTable> groupRemoved = entry.getValue().left;
            Set<CompactionSSTable> groupAdded = entry.getValue().right;

            if (!groupRemoved.isEmpty())
                strategy.replaceSSTables(groupRemoved, groupAdded);
            else
                strategy.addSSTables(groupAdded);

            removeSessionIfEmpty(entry.getKey());
        }
    }

    synchronized void startup()
    {
        strategies.values().forEach(CompactionStrategy::startup);
    }

    synchronized void shutdown()
    {
        strategies.values().forEach(CompactionStrategy::shutdown);
    }

    private int getEstimatedRemainingTasks(TimeUUID sessionID, AbstractCompactionStrategy strategy)
    {
        return getEstimatedRemainingTasks(sessionID, strategy, 0, 0);
    }

    private int getEstimatedRemainingTasks(TimeUUID sessionID, AbstractCompactionStrategy strategy, int additionalSSTables, long additionalBytes)
    {
        return canCleanup(sessionID) ? 0 : strategy.getEstimatedRemainingTasks();
    }

    int getEstimatedRemainingTasks()
    {
        return getEstimatedRemainingTasks(0, 0);
    }

    int getEstimatedRemainingTasks(int additionalSSTables, long additionalBytes)
    {
        int tasks = 0;
        for (Map.Entry<TimeUUID, LegacyAbstractCompactionStrategy> entry : strategies.entrySet())
        {
            tasks += getEstimatedRemainingTasks(entry.getKey(), entry.getValue(), additionalSSTables, additionalBytes);
        }
        return tasks;
    }

    /**
     * @return the highest max remaining tasks of all contained compaction strategies
     */
    int getMaxEstimatedRemainingTasks()
    {
        int tasks = 0;
        for (Map.Entry<TimeUUID, LegacyAbstractCompactionStrategy> entry : strategies.entrySet())
        {
            tasks = Math.max(tasks, getEstimatedRemainingTasks(entry.getKey(), entry.getValue()));
        }
        return tasks;
    }

    private RepairFinishedCompactionTask getRepairFinishedCompactionTask(TimeUUID sessionID)
    {
        Preconditions.checkState(canCleanup(sessionID));
        LegacyAbstractCompactionStrategy compactionStrategy = get(sessionID);
        if (compactionStrategy == null)
            return null;
        Set<? extends CompactionSSTable> sstables = compactionStrategy.getSSTables();
        long repairedAt = ActiveRepairService.instance().consistent.local.getFinalSessionRepairedAt(sessionID);
        LifecycleTransaction txn = realm.tryModify(sstables, OperationType.COMPACTION);
        return txn == null ? null : new RepairFinishedCompactionTask(realm, txn, sessionID, repairedAt, isTransient);
    }

    public CleanupTask releaseSessionData(Collection<TimeUUID> sessionIDs)
    {
        List<Pair<TimeUUID, RepairFinishedCompactionTask>> tasks = new ArrayList<>(sessionIDs.size());
        for (TimeUUID session : sessionIDs)
        {
            if (hasDataForSession(session))
            {
                tasks.add(Pair.create(session, getRepairFinishedCompactionTask(session)));
            }
        }
        return new CleanupTask(realm, tasks);
    }

    synchronized int getNumPendingRepairFinishedTasks()
    {
        int count = 0;
        for (TimeUUID sessionID : strategies.keySet())
        {
            if (canCleanup(sessionID))
            {
                count++;
            }
        }
        return count;
    }

    synchronized Collection<AbstractCompactionTask> getNextRepairFinishedTasks()
    {
        for (TimeUUID sessionID : strategies.keySet())
        {
            if (canCleanup(sessionID))
            {
                RepairFinishedCompactionTask task = getRepairFinishedCompactionTask(sessionID);
                if (task != null)
                    return ImmutableList.of(task);
                else
                    return ImmutableList.of();
            }
        }
        return ImmutableList.of();
    }

    synchronized Collection<AbstractCompactionTask> getNextBackgroundTasks(long gcBefore)
    {
        if (strategies.isEmpty())
            return ImmutableList.of();
        Map<TimeUUID, Integer> numTasks = new HashMap<>(strategies.size());
        ArrayList<TimeUUID> sessions = new ArrayList<>(strategies.size());
        for (Map.Entry<TimeUUID, LegacyAbstractCompactionStrategy> entry : strategies.entrySet())
        {
            if (canCleanup(entry.getKey()))
            {
                continue;
            }
            numTasks.put(entry.getKey(), getEstimatedRemainingTasks(entry.getKey(), entry.getValue()));
            sessions.add(entry.getKey());
        }

        if (sessions.isEmpty())
            return ImmutableList.of();

        // we want the session with the most compactions at the head of the list
        sessions.sort((o1, o2) -> numTasks.get(o2) - numTasks.get(o1));

        TimeUUID sessionID = sessions.get(0);
        return get(sessionID).getNextBackgroundTasks(gcBefore);
    }

    synchronized Collection<AbstractCompactionTask> getMaximalTasks(long gcBefore, boolean splitOutput)
    {
        if (strategies.isEmpty())
            return ImmutableList.of();

        List<AbstractCompactionTask> maximalTasks = new ArrayList<>(strategies.size());
        for (Map.Entry<TimeUUID, LegacyAbstractCompactionStrategy> entry : strategies.entrySet())
        {
            if (canCleanup(entry.getKey()))
            {
                maximalTasks.add(getRepairFinishedCompactionTask(entry.getKey()));
            }
            else
            {
                maximalTasks.addAll(entry.getValue().getMaximalTasks(gcBefore, splitOutput));
            }
        }
        return maximalTasks;
    }

    Collection<LegacyAbstractCompactionStrategy> getStrategies()
    {
        return strategies.values();
    }

    Set<TimeUUID> getSessions()
    {
        return strategies.keySet();
    }

    boolean canCleanup(TimeUUID sessionID)
    {
        return !ActiveRepairService.instance().consistent.local.isSessionInProgress(sessionID);
    }

    synchronized Set<ISSTableScanner> getScanners(Collection<SSTableReader> sstables, Collection<Range<Token>> ranges)
    {
        if (sstables.isEmpty())
        {
            return Collections.emptySet();
        }

        Map<TimeUUID, Set<SSTableReader>> sessionSSTables = new HashMap<>();
        for (SSTableReader sstable : sstables)
        {
            TimeUUID sessionID = sstable.getPendingRepair();
            checkPendingID(sessionID);
            sessionSSTables.computeIfAbsent(sessionID, k -> new HashSet<>()).add(sstable);
        }

        Set<ISSTableScanner> scanners = new HashSet<>(sessionSSTables.size());
        try
        {
            for (Map.Entry<TimeUUID, Set<SSTableReader>> entry : sessionSSTables.entrySet())
            {
                scanners.addAll(getOrCreate(entry.getKey()).getScanners(entry.getValue(), ranges).scanners);
            }
        }
        catch (Throwable t)
        {
            ISSTableScanner.closeAllAndPropagate(scanners, t);
        }
        return scanners;
    }

    public boolean hasStrategy(CompactionStrategy strategy)
    {
        return strategies.values().contains(strategy);
    }

    public synchronized boolean hasDataForSession(TimeUUID sessionID)
    {
        return strategies.containsKey(sessionID);
    }

    boolean containsSSTable(CompactionSSTable sstable)
    {
        if (!sstable.isPendingRepair())
            return false;

        AbstractCompactionStrategy strategy = strategies.get(sstable.getPendingRepair());
        return strategy != null && strategy.getSSTables().contains(sstable);
    }

    public Collection<AbstractCompactionTask> createUserDefinedTasks(Collection<CompactionSSTable> sstables, long gcBefore)
    {
        Map<TimeUUID, List<CompactionSSTable>> group = sstables.stream().collect(Collectors.groupingBy(s -> s.getPendingRepair()));
        return group.entrySet().stream().map(g -> strategies.get(g.getKey()).getUserDefinedTasks(g.getValue(), gcBefore)).flatMap(Collection::stream).collect(Collectors.toList());
    }
}

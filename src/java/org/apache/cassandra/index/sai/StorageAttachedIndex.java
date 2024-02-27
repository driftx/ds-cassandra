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

package org.apache.cassandra.index.sai;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.ResultSet;
import org.apache.cassandra.cql3.restrictions.Restriction;
import org.apache.cassandra.cql3.restrictions.SingleColumnRestriction;
import org.apache.cassandra.cql3.statements.schema.IndexTarget;
import org.apache.cassandra.db.CassandraWriteContext;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.WriteContext;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.compaction.TableOperation;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.lifecycle.LifecycleNewTracker;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.VectorType;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.dht.OrderPreservingPartitioner;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.IndexBuildDecider;
import org.apache.cassandra.index.IndexRegistry;
import org.apache.cassandra.index.SecondaryIndexBuilder;
import org.apache.cassandra.index.SecondaryIndexManager;
import org.apache.cassandra.index.TargetParser;
import org.apache.cassandra.index.sai.analyzer.AbstractAnalyzer;
import org.apache.cassandra.index.sai.analyzer.LuceneAnalyzer;
import org.apache.cassandra.index.sai.analyzer.NonTokenizingOptions;
import org.apache.cassandra.index.sai.disk.StorageAttachedIndexWriter;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.disk.v1.IndexWriterConfig;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.cassandra.index.sai.view.View;
import org.apache.cassandra.index.transactions.IndexTransaction;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableFlushObserver;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

import static org.apache.cassandra.config.CassandraRelevantProperties.SAI_VALIDATE_TERMS_AT_COORDINATOR;
import static org.apache.cassandra.index.sai.disk.v1.IndexWriterConfig.MAX_TOP_K;

public class StorageAttachedIndex implements Index
{
    private static final Logger logger = LoggerFactory.getLogger(StorageAttachedIndex.class);

    private static final boolean VALIDATE_TERMS_AT_COORDINATOR = SAI_VALIDATE_TERMS_AT_COORDINATOR.getBoolean();

    private static class StorageAttachedIndexBuildingSupport implements IndexBuildingSupport
    {
        public NavigableMap<SSTableReader, Set<StorageAttachedIndex>> prepareSSTablesToBuild(StorageAttachedIndexGroup group,
                                                       Set<Index> indexes,
                                                       Collection<SSTableReader> sstablesToRebuild,
                                                       boolean isFullRebuild)
        {
            NavigableMap<SSTableReader, Set<StorageAttachedIndex>> sstables = new TreeMap<>(SSTableReader.idComparator);

            indexes.stream()
                   .filter((i) -> i instanceof StorageAttachedIndex)
                   .forEach((i) ->
                            {
                                StorageAttachedIndex sai = (StorageAttachedIndex) i;
                                IndexContext indexContext = ((StorageAttachedIndex) i).getIndexContext();

                                // If this is not a full manual index rebuild we can skip SSTables that already have an
                                // attached index. Otherwise, we override any pre-existent index.
                                Collection<SSTableReader> ss = sstablesToRebuild;
                                if (!isFullRebuild)
                                {
                                    ss = sstablesToRebuild.stream()
                                                          .filter(s -> !IndexDescriptor.create(s).isPerIndexBuildComplete(indexContext))
                                                          .collect(Collectors.toList());
                                }

                                group.dropIndexSSTables(ss, sai);

                                ss.forEach((sstable) ->
                                           {
                                               Set<StorageAttachedIndex> toBuild = sstables.get(sstable);
                                               if (toBuild == null) sstables.put(sstable, (toBuild = new HashSet<>()));
                                               toBuild.add(sai);
                                           });
                            });

            return sstables;
        }

        @Override
        public SecondaryIndexBuilder getIndexBuildTask(ColumnFamilyStore cfs, Set<Index> indexes, Collection<SSTableReader> sstablesToRebuild, boolean isFullRebuild)
        {
            StorageAttachedIndexGroup group = StorageAttachedIndexGroup.getIndexGroup(cfs);
            NavigableMap<SSTableReader, Set<StorageAttachedIndex>> sstables = prepareSSTablesToBuild(group, indexes, sstablesToRebuild, isFullRebuild);
            return new StorageAttachedIndexBuilder(group, sstables, isFullRebuild, false);
        }

        @Override
        public List<SecondaryIndexBuilder> getParallelIndexBuildTasks(ColumnFamilyStore cfs, Set<Index> indexes, Collection<SSTableReader> sstablesToRebuild, boolean isFullRebuild)
        {
            StorageAttachedIndexGroup indexGroup = StorageAttachedIndexGroup.getIndexGroup(cfs);
            NavigableMap<SSTableReader, Set<StorageAttachedIndex>> sstables = prepareSSTablesToBuild(indexGroup, indexes, sstablesToRebuild, isFullRebuild);

            List<List<SSTableReader>> groups = groupBySize(new ArrayList<>(sstables.keySet()), DatabaseDescriptor.getConcurrentCompactors());
            List<SecondaryIndexBuilder> builders = new ArrayList<>();

            for (List<SSTableReader> group : groups)
            {
                SortedMap<SSTableReader, Set<StorageAttachedIndex>> current = new TreeMap<>(Comparator.comparing(sstable -> sstable.descriptor.id));
                group.forEach(sstable -> current.put(sstable, sstables.get(sstable)));

                builders.add(new StorageAttachedIndexBuilder(indexGroup, current, isFullRebuild, false));
            }

            logger.info("Creating {} parallel index builds over {} total sstables for {}...", builders.size(), sstables.size(), cfs.metadata());

            return builders;
        }
    }

    // Used to build indexes on newly added SSTables:
    private static final StorageAttachedIndexBuildingSupport INDEX_BUILDER_SUPPORT = new StorageAttachedIndexBuildingSupport();

    private static final Set<String> VALID_OPTIONS = ImmutableSet.of(NonTokenizingOptions.CASE_SENSITIVE,
                                                                     NonTokenizingOptions.NORMALIZE,
                                                                     NonTokenizingOptions.ASCII,
                                                                     // For now, we leave this for backward compatibility even though it's not used
                                                                     IndexContext.ENABLE_SEGMENT_COMPACTION_OPTION_NAME,
                                                                     IndexTarget.TARGET_OPTION_NAME,
                                                                     IndexTarget.CUSTOM_INDEX_OPTION_NAME,
                                                                     IndexWriterConfig.POSTING_LIST_LVL_MIN_LEAVES,
                                                                     IndexWriterConfig.POSTING_LIST_LVL_SKIP_OPTION,
                                                                     IndexWriterConfig.MAXIMUM_NODE_CONNECTIONS,
                                                                     IndexWriterConfig.CONSTRUCTION_BEAM_WIDTH,
                                                                     IndexWriterConfig.SIMILARITY_FUNCTION,
                                                                     IndexWriterConfig.SOURCE_MODEL,
                                                                     IndexWriterConfig.OPTIMIZE_FOR,
                                                                     LuceneAnalyzer.INDEX_ANALYZER,
                                                                     LuceneAnalyzer.QUERY_ANALYZER);

    // this does not include vectors because each Vector declaration is a separate type instance
    public static final Set<CQL3Type> SUPPORTED_TYPES = ImmutableSet.of(CQL3Type.Native.ASCII, CQL3Type.Native.BIGINT, CQL3Type.Native.DATE,
                                                                        CQL3Type.Native.DOUBLE, CQL3Type.Native.FLOAT, CQL3Type.Native.INT,
                                                                        CQL3Type.Native.SMALLINT, CQL3Type.Native.TEXT, CQL3Type.Native.TIME,
                                                                        CQL3Type.Native.TIMESTAMP, CQL3Type.Native.TIMEUUID, CQL3Type.Native.TINYINT,
                                                                        CQL3Type.Native.UUID, CQL3Type.Native.VARCHAR, CQL3Type.Native.INET,
                                                                        CQL3Type.Native.VARINT, CQL3Type.Native.DECIMAL, CQL3Type.Native.BOOLEAN);

    private static final Set<Class<? extends IPartitioner>> ILLEGAL_PARTITIONERS =
            ImmutableSet.of(OrderPreservingPartitioner.class, LocalPartitioner.class, ByteOrderedPartitioner.class, RandomPartitioner.class);

    private final ColumnFamilyStore baseCfs;
    private final IndexMetadata config;
    private final IndexContext indexContext;

    // Tracks whether or not we've started the index build on initialization.
    private volatile boolean canFlushFromMemtableIndex = false;

    // Tracks whether the index has been invalidated due to removal, a table drop, etc.
    private volatile boolean valid = true;

    /**
     * Called via reflection from SecondaryIndexManager
     */
    public StorageAttachedIndex(ColumnFamilyStore baseCfs, IndexMetadata config)
    {
        this.baseCfs = baseCfs;
        this.config = config;
        TableMetadata tableMetadata = baseCfs.metadata();
        Pair<ColumnMetadata, IndexTarget.Type> target = TargetParser.parse(tableMetadata, config);
        this.indexContext = new IndexContext(tableMetadata.keyspace,
                                             tableMetadata.name,
                                             tableMetadata.partitionKeyType,
                                             tableMetadata.comparator,
                                             target.left,
                                             target.right,
                                             config,
                                             baseCfs);
    }

    /**
     * Used via reflection in {@link IndexMetadata}
     */
    @SuppressWarnings({ "unused" })
    public static Map<String, String> validateOptions(Map<String, String> options, TableMetadata metadata)
    {
        Map<String, String> unknown = new HashMap<>(2);

        for (Map.Entry<String, String> option : options.entrySet())
        {
            if (!VALID_OPTIONS.contains(option.getKey()))
            {
                unknown.put(option.getKey(), option.getValue());
            }
        }

        if (!unknown.isEmpty())
        {
            return unknown;
        }

        if (ILLEGAL_PARTITIONERS.contains(metadata.partitioner.getClass()))
        {
            throw new InvalidRequestException("Storage-attached index does not support the following IPartitioner implementations: " + ILLEGAL_PARTITIONERS);
        }

        String targetColumn = options.get(IndexTarget.TARGET_OPTION_NAME);

        if (targetColumn == null)
        {
            throw new InvalidRequestException("Missing target column");
        }

        if (targetColumn.split(",").length > 1)
        {
            throw new InvalidRequestException("A storage-attached index cannot be created over multiple columns: " + targetColumn);
        }

        Pair<ColumnMetadata, IndexTarget.Type> target = TargetParser.parse(metadata, targetColumn);

        if (target == null)
        {
            throw new InvalidRequestException("Failed to retrieve target column for: " + targetColumn);
        }

        // In order to support different index target on non-frozen map, ie. KEYS, VALUE, ENTRIES, we need to put index
        // name as part of index file name instead of column name. We only need to check that the target is different
        // between indexes. This will only allow indexes in the same column with a different IndexTarget.Type.
        //
        // Note that: "metadata.indexes" already includes current index
        if (metadata.indexes.stream().filter(index -> index.getIndexClassName().equals(StorageAttachedIndex.class.getName()))
                            .map(index -> TargetParser.parse(metadata, index.options.get(IndexTarget.TARGET_OPTION_NAME)))
                            .filter(Objects::nonNull).filter(t -> t.equals(target)).count() > 1)
        {
            throw new InvalidRequestException("Cannot create more than one storage-attached index on the same column: " + target.left);
        }

        // Analyzer is not supported against PK columns
        if (AbstractAnalyzer.isAnalyzed(options))
        {
            for (ColumnMetadata column : metadata.primaryKeyColumns())
            {
                if (column.name.equals(target.left.name))
                    throw new InvalidRequestException("Cannot specify index analyzer on primary key column: " + target.left);
            }
        }

        AbstractType<?> type = TypeUtil.cellValueType(target.left, target.right);
        AbstractAnalyzer.fromOptions(type, options); // will throw if invalid
        var config = IndexWriterConfig.fromOptions(null, type, options);

        // If we are indexing map entries we need to validate the sub-types
        if (TypeUtil.isComposite(type))
        {
            for (AbstractType<?> subType : type.subTypes())
            {
                if (!SUPPORTED_TYPES.contains(subType.asCQL3Type()) && !TypeUtil.isFrozen(subType))
                    throw new InvalidRequestException("Unsupported composite type for SAI: " + subType.asCQL3Type());
            }
        }
        else if (type.isVector())
        {
            if (type.valueLengthIfFixed() == 4 && config.getSimilarityFunction() == VectorSimilarityFunction.COSINE)
                throw new InvalidRequestException("Cosine similarity is not supported for single-dimension vectors");

            // vectors of fixed length types are fixed length too, so we can reject the index creation
            // if that fixed length is over the max term size for vectors
            if (type.isValueLengthFixed() && IndexContext.MAX_VECTOR_TERM_SIZE < type.valueLengthIfFixed())
            {
                AbstractType<?> elementType = ((VectorType<?>) type).elementType;
                var error = String.format("Vector index created with %s will produce terms of %s, " +
                                          "exceeding the max vector term size of %s. " +
                                          "That sets an implicit limit of %d dimensions for %s vectors.",
                                          type.asCQL3Type(),
                                          FBUtilities.prettyPrintMemory(type.valueLengthIfFixed()),
                                          FBUtilities.prettyPrintMemory(IndexContext.MAX_VECTOR_TERM_SIZE),
                                          IndexContext.MAX_VECTOR_TERM_SIZE / elementType.valueLengthIfFixed(),
                                          elementType.asCQL3Type());
                // VSTODO until we can safely differentiate client and system requests, we can only log here
                // Ticket for this: https://github.com/riptano/VECTOR-SEARCH/issues/85
                logger.warn(error);
            }
        }
        else if (!SUPPORTED_TYPES.contains(type.asCQL3Type()) && !TypeUtil.isFrozen(type))
        {
            throw new InvalidRequestException("Unsupported type for SAI: " + type.asCQL3Type());
        }

        return Collections.emptyMap();
    }

    @Override
    public void register(IndexRegistry registry)
    {
        // index will be available for writes
        registry.registerIndex(this, StorageAttachedIndexGroup.GROUP_KEY, () -> new StorageAttachedIndexGroup(baseCfs));
    }

    @Override
    public void unregister(IndexRegistry registry)
    {
        registry.unregisterIndex(this, StorageAttachedIndexGroup.GROUP_KEY);
    }

    @Override
    public IndexMetadata getIndexMetadata()
    {
        return config;
    }

    @Override
    public boolean shouldSkipInitialization()
    {
        // SAI performs partial initialization so it must always execute it; the actual index build is then still skipped
        // if IndexBuildDecider.instance.onInitialBuild().skipped() is true.
        return false;
    }

    @Override
    public Callable<?> getInitializationTask()
    {
        IndexBuildDecider.Decision decision = IndexBuildDecider.instance.onInitialBuild();
        // New storage-attached indexes will be available for queries after on disk index data are built.
        // Memtable data will be indexed via flushing triggered by schema change
        // We only want to validate the index files if we are starting up
        return () -> startInitialBuild(baseCfs, StorageService.instance.isStarting(), decision.skipped()).get();
    }

    private Future<?> startInitialBuild(ColumnFamilyStore baseCfs, boolean validate, boolean skipIndexBuild)
    {
        if (skipIndexBuild)
        {
            logger.info("Skipping initialization task for {}.{} after flushing memtable", baseCfs.metadata(), indexContext.getIndexName());
            // Force another flush to make sure on disk index is generated for memtable data before marking it queryable.
            // In case of offline scrub, there is no live memtables.
            if (!baseCfs.getTracker().getView().liveMemtables.isEmpty())
                baseCfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.INDEX_BUILD_STARTED);

            // From now on, all memtable will have attached memtable index. It is now safe to flush indexes directly from flushing Memtables.
            canFlushFromMemtableIndex = true;
            return CompletableFuture.completedFuture(null);
        }

        if (baseCfs.indexManager.isIndexQueryable(this))
        {
            logger.debug(indexContext.logMessage("Skipping validation and building in initialization task, as pre-join has already made the storage attached index queryable..."));
            canFlushFromMemtableIndex = true;
            return CompletableFuture.completedFuture(null);
        }

        // stop in-progress compaction tasks to prevent compacted sstable not being index.
        logger.debug(indexContext.logMessage("Stopping active compactions to make sure all sstables are indexed after initial build."));
        CompactionManager.instance.interruptCompactionFor(Collections.singleton(baseCfs.metadata()),
                                                          OperationType.REWRITES_SSTABLES,
                                                          Predicates.alwaysTrue(),
                                                          true,
                                                          TableOperation.StopTrigger.INDEX_BUILD);

        // Force another flush to make sure on disk index is generated for memtable data before marking it queryable.
        // In case of offline scrub, there is no live memtables.
        if (!baseCfs.getTracker().getView().liveMemtables.isEmpty())
        {
            baseCfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.INDEX_BUILD_STARTED);
        }

        // From now on, all memtable will have attached memtable index. It is now safe to flush indexes directly from flushing Memtables.
        canFlushFromMemtableIndex = true;

        StorageAttachedIndexGroup indexGroup = StorageAttachedIndexGroup.getIndexGroup(baseCfs);
        List<SSTableReader> nonIndexed = findNonIndexedSSTables(baseCfs, indexGroup, validate);

        if (nonIndexed.isEmpty())
        {
            return CompletableFuture.completedFuture(null);
        }

        // split sorted sstables into groups with similar size and build each group in separate compaction thread
        List<List<SSTableReader>> groups = groupBySize(nonIndexed, DatabaseDescriptor.getConcurrentCompactors());
        List<ListenableFuture<?>> futures = new ArrayList<>();

        for (List<SSTableReader> group : groups)
        {
            SortedMap<SSTableReader, Set<StorageAttachedIndex>> current = new TreeMap<>(SSTableReader.idComparator);
            group.forEach(sstable -> current.put(sstable, Collections.singleton(this)));

            futures.add(CompactionManager.instance.submitIndexBuild(new StorageAttachedIndexBuilder(indexGroup, current, false, true)));
        }

        logger.info(indexContext.logMessage("Submitting {} parallel initial index builds over {} total sstables..."), futures.size(), nonIndexed.size());
        return Futures.allAsList(futures);
    }

    /**
     * Splits SSTables into groups of similar overall size.
     *
     * @param toRebuild a list of SSTables to split (Note that this list will be sorted in place!)
     * @param parallelism an upper bound on the number of groups
     *
     * @return a {@link List} of SSTable groups, each represented as a {@link List} of {@link SSTableReader}
     */
    @VisibleForTesting
    public static List<List<SSTableReader>> groupBySize(List<SSTableReader> toRebuild, int parallelism)
    {
        List<List<SSTableReader>> groups = new ArrayList<>();

        toRebuild.sort(Comparator.comparingLong(SSTableReader::onDiskLength).reversed());
        Iterator<SSTableReader> sortedSSTables = toRebuild.iterator();
        double dataPerCompactor = toRebuild.stream().mapToLong(SSTableReader::onDiskLength).sum() * 1.0 / parallelism;

        while (sortedSSTables.hasNext())
        {
            long sum = 0;
            List<SSTableReader> current = new ArrayList<>();

            while (sortedSSTables.hasNext() && sum < dataPerCompactor)
            {
                SSTableReader sstable = sortedSSTables.next();
                sum += sstable.onDiskLength();
                current.add(sstable);
            }

            assert !current.isEmpty();
            groups.add(current);
        }

        return groups;
    }

    @Override
    public Callable<?> getMetadataReloadTask(IndexMetadata indexMetadata)
    {
        return null;
    }

    @Override
    public Callable<?> getBlockingFlushTask()
    {
        return null; // storage-attached indexes are flushed alongside memtable
    }

    @Override
    public Callable<?> getInvalidateTask()
    {
        return () ->
        {
            // mark index as invalid, in-progress SSTableIndexWriters will abort
            valid = false;

            // in case of dropping table, SSTable indexes should already been removed by SSTableListChangedNotification.
            Set<Component> toRemove = getComponents();
            for (SSTableIndex sstableIndex : indexContext.getView().getIndexes())
                sstableIndex.getSSTable().unregisterComponents(toRemove, baseCfs.getTracker());

            indexContext.invalidate(true);
            return null;
        };
    }

    @Override
    public Callable<?> getUnloadTask()
    {
        return () ->
        {
            // mark index as invalid, in-progress SSTableIndexWriters will abort
            valid = false;

            indexContext.invalidate(false);
            return null;
        };
    }

    @Override
    public Callable<?> getPreJoinTask(boolean hadBootstrap)
    {
        /*
         * During bootstrap, streamed SSTable are already built for existing indexes via {@link StorageAttachedIndexBuildingSupport}
         * from {@link org.apache.cassandra.streaming.StreamReceiveTask.OnCompletionRunnable}.
         *
         * For indexes created during bootstrapping, we don't have to block bootstrap for them.
         */

        return this::startPreJoinTask;
    }

    @VisibleForTesting
    public boolean canFlushFromMemtableIndex()
    {
        return canFlushFromMemtableIndex;
    }

    public BooleanSupplier isIndexValid()
    {
        return () -> valid;
    }

    private Future<?> startPreJoinTask()
    {
        try
        {
            if (baseCfs.indexManager.isIndexQueryable(this))
            {
                logger.debug(indexContext.logMessage("Skipping validation in pre-join task, as the initialization task has already made the index queryable..."));
                baseCfs.indexManager.makeIndexQueryable(this, Status.BUILD_SUCCEEDED);
                return null;
            }

            StorageAttachedIndexGroup group = StorageAttachedIndexGroup.getIndexGroup(baseCfs);
            Collection<SSTableReader> nonIndexed = findNonIndexedSSTables(baseCfs, group, true);

            if (nonIndexed.isEmpty())
            {
                // If the index is complete, mark it queryable before the node starts accepting requests:
                baseCfs.indexManager.makeIndexQueryable(this, Status.BUILD_SUCCEEDED);
            }
        }
        catch (Throwable t)
        {
            logger.error(indexContext.logMessage("Failed in pre-join task!"), t);
        }

        return null;
    }

    @Override
    public Callable<?> getTruncateTask(long truncatedAt)
    {
        /*
         * index files will be removed as part of base sstable lifecycle in {@link LogTransaction#delete(java.io.File)}
         * asynchronously, but we need to mark the index queryable because if the truncation is during the initial
         * build of the index it won't get marked queryable by the build.
         */
        return () -> {
            logger.info(indexContext.logMessage("Making index queryable during table truncation"));
            baseCfs.indexManager.makeIndexQueryable(this, Status.BUILD_SUCCEEDED);
            return null;
        };
    }

    @Override
    public boolean shouldBuildBlocking()
    {
        return true;
    }

    @Override
    public Optional<ColumnFamilyStore> getBackingTable()
    {
        return Optional.empty();
    }

    @Override
    public boolean dependsOn(ColumnMetadata column)
    {
        return indexContext.getDefinition().compareTo(column) == 0;
    }

    @Override
    public boolean supportsExpression(ColumnMetadata column, Operator operator)
    {
        return dependsOn(column) && indexContext.supports(operator);
    }

    @Override
    public AbstractType<?> customExpressionValueType()
    {
        return null;
    }

    @Override
    public RowFilter getPostIndexQueryFilter(RowFilter filter)
    {
        // it should be executed from the SAI query plan, this is only used by the singleton index query plan
        throw new UnsupportedOperationException();
    }

    @Override
    public void postQuerySort(ResultSet cqlRows, Restriction restriction, int columnIndex, QueryOptions options)
    {
        // For now, only support ANN
        assert restriction instanceof SingleColumnRestriction.AnnRestriction;

        Preconditions.checkState(indexContext.isVector());

        SingleColumnRestriction.AnnRestriction annRestriction = (SingleColumnRestriction.AnnRestriction) restriction;
        VectorSimilarityFunction similarityFunction = indexContext.getIndexWriterConfig().getSimilarityFunction();

        float[] targetVector = TypeUtil.decomposeVector(indexContext, annRestriction.value(options).duplicate());

        List<List<ByteBuffer>> buffRows = cqlRows.rows;
        // Decorate-sort-undecorate to optimize sorting of vectors by their similarity scores
        List<Pair<List<ByteBuffer>, Double>> listPairsVectorsScores = buffRows.stream()
                                                                              .map(row -> {
                                                                                  ByteBuffer vectorBuffer = row.get(columnIndex);
                                                                                  float[] vector = TypeUtil.decomposeVector(indexContext, vectorBuffer.duplicate());
                                                                                  Double score = (double) similarityFunction.compare(vector, targetVector);
                                                                                  return Pair.create(row, score);
                                                                              })
                                                                              .collect(Collectors.toList());
        listPairsVectorsScores.sort(Comparator.comparing(pair -> pair.right, Comparator.reverseOrder()));
        List<List<ByteBuffer>> sortedRows = listPairsVectorsScores.stream()
                                                                  .map(pair -> pair.left)
                                                                  .collect(Collectors.toList());

        cqlRows.rows = sortedRows;
    }

    @Override
    public void validate(ReadCommand command) throws InvalidRequestException
    {
        var indexQueryPlan = command.indexQueryPlan();
        if (indexQueryPlan == null || !indexQueryPlan.isTopK())
            return;

        // to avoid overflow HNSW internal data structure and avoid OOM when filtering top-k
        if (command.limits().isUnlimited() || command.limits().count() > MAX_TOP_K)
            throw new InvalidRequestException(String.format("Use of ANN OF in an ORDER BY clause requires a LIMIT that is not greater than %s. LIMIT was %s",
                                                            MAX_TOP_K, command.limits().isUnlimited() ? "NO LIMIT" : command.limits().count()));

        indexContext.validate(command.rowFilter());
    }

    @Override
    public long getEstimatedResultRows()
    {
        throw new UnsupportedOperationException("Use StorageAttachedIndexQueryPlan#getEstimatedResultRows() instead.");
    }

    @Override
    public boolean isQueryable(Status status)
    {
        return status == Status.BUILD_SUCCEEDED || status == Status.UNKNOWN;
    }

    @Override
    public void validate(PartitionUpdate update) throws InvalidRequestException
    {
        if (!VALIDATE_TERMS_AT_COORDINATOR)
            return;

        DecoratedKey key = update.partitionKey();
        for (Row row : update)
            indexContext.validate(key, row);
    }

    /**
     * This method is called by the startup tasks to find SSTables that don't have indexes. The method is
     * synchronized so that the view is unchanged between validation and the selection of non-indexed SSTables.
     *
     * @return a list SSTables without attached indexes
     */
    private synchronized List<SSTableReader> findNonIndexedSSTables(ColumnFamilyStore baseCfs, StorageAttachedIndexGroup group, boolean validate)
    {
        Set<SSTableReader> sstables = baseCfs.getLiveSSTables();

        // Initialize the SSTable indexes w/ valid existing components...
        assert group != null : "Missing index group on " + baseCfs.name;
        group.onSSTableChanged(Collections.emptyList(), sstables, Collections.singleton(this), validate);

        // ...then identify and rebuild the SSTable indexes that are missing.
        List<SSTableReader> nonIndexed = new ArrayList<>();
        View view = indexContext.getView();

        for (SSTableReader sstable : sstables)
        {
            // An SSTable is considered not indexed if:
            //   1. The current view does not contain the SSTable
            //   2. The SSTable is not marked compacted
            //   3. The column index does not have a completion marker
            if (!view.containsSSTable(sstable) && !sstable.isMarkedCompacted() &&
                !IndexDescriptor.create(sstable).isPerIndexBuildComplete(indexContext))
            {
                nonIndexed.add(sstable);
            }
        }

        return nonIndexed;
    }

    private class UpdateIndexer extends IndexerAdapter
    {
        private final DecoratedKey key;
        private final Memtable mt;
        private final WriteContext writeContext;

        UpdateIndexer(DecoratedKey key, Memtable mt, WriteContext writeContext)
        {
            this.key = key;
            this.mt = mt;
            this.writeContext = writeContext;
        }

        @Override
        public void insertRow(Row row)
        {
            indexContext.index(key, row, mt, CassandraWriteContext.fromContext(writeContext).getGroup());
        }

        @Override
        public void updateRow(Row oldRow, Row newRow)
        {
            indexContext.update(key, oldRow, newRow, mt, CassandraWriteContext.fromContext(writeContext).getGroup());
        }
    }

    protected static abstract class IndexerAdapter implements Indexer
    {
        @Override
        public void begin() { }

        @Override
        public void finish() { }

        @Override
        public void partitionDelete(DeletionTime dt)
        {
        }

        @Override
        public void rangeTombstone(RangeTombstone rt)
        {
        }

        @Override
        public void removeRow(Row row)
        {
        }
    }

    @Override
    public Searcher searcherFor(ReadCommand command) throws InvalidRequestException
    {
        // searchers should be created from the query plan, this is only used by the singleton index query plan
        throw new UnsupportedOperationException();
    }

    @Override
    public SSTableFlushObserver getFlushObserver(Descriptor descriptor, LifecycleNewTracker tracker)
    {
        throw new UnsupportedOperationException("Storage-attached index flush observers should never be created directly.");
    }

    @Override
    public Set<Component> getComponents()
    {
        return Version.LATEST.onDiskFormat()
                             .perIndexComponents(indexContext)
                             .stream()
                             .map(c -> new Component(Component.Type.CUSTOM,
                                                     Version.LATEST.fileNameFormatter().format(c, indexContext)))
                             .collect(Collectors.toSet());
    }

    @Override
    public Indexer indexerFor(DecoratedKey key,
                              RegularAndStaticColumns columns,
                              int nowInSec,
                              WriteContext writeContext,
                              IndexTransaction.Type transactionType,
                              Memtable memtable)
    {
        if (transactionType == IndexTransaction.Type.UPDATE)
        {
            return new UpdateIndexer(key, memtable, writeContext);
        }

        // we are only interested in the data from Memtable
        // everything else is going to be handled by SSTableWriter observers
        return null;
    }

    @Override
    public IndexBuildingSupport getBuildTaskSupport()
    {
        return INDEX_BUILDER_SUPPORT;
    }

    public IndexContext getIndexContext()
    {
        return indexContext;
    }

    @Override
    public String toString()
    {
        return String.format("%s.%s.%s", baseCfs.keyspace.getName(), baseCfs.name, config == null ? "?" : config.name);
    }

    /**
     * Removes this index from the {@link SecondaryIndexManager}'s set of queryable indexes.
     *
     * This usually happens in response to an index writing failure from {@link StorageAttachedIndexWriter}.
     */
    public void makeIndexNonQueryable()
    {
        baseCfs.indexManager.makeIndexNonQueryable(this, Status.BUILD_FAILED);
        logger.warn(indexContext.logMessage("Storage-attached index is no longer queryable. Please restart this node to repair it."));
    }
}

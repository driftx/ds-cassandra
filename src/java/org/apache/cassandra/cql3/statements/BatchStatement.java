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
package org.apache.cassandra.cql3.statements;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.UnaryOperator;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.helpers.MessageFormatter;

import org.apache.cassandra.audit.AuditLogContext;
import org.apache.cassandra.audit.AuditLogEntryType;
import org.apache.cassandra.cql3.Attributes;
import org.apache.cassandra.cql3.BatchQueryOptions;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.ResultSet;
import org.apache.cassandra.cql3.VariableSpecifications;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.IMutation;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.guardrails.Guardrails;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.metrics.BatchMetrics;
import org.apache.cassandra.metrics.ClientRequestSizeMetrics;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.NoSpamLogger;
import org.apache.cassandra.utils.Pair;

import static java.util.function.Predicate.isEqual;
import static org.apache.cassandra.cql3.statements.RequestValidations.checkFalse;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;

/**
 * A <code>BATCH</code> statement parsed from a CQL query.
 */
public class BatchStatement implements CQLStatement
{
    public enum Type
    {
        LOGGED, UNLOGGED, COUNTER
    }

    private final String rawCQLStatement;
    public final Type type;
    private final VariableSpecifications bindVariables;
    private final List<ModificationStatement> statements;

    // Columns modified for each table (keyed by the table ID)
    private final Map<TableId, RegularAndStaticColumns> updatedColumns;
    // Columns on which there is conditions. Note that if there is any, then the batch can only be on a single partition (and thus table).
    private final RegularAndStaticColumns conditionColumns;

    private final boolean updatesRegularRows;
    private final boolean updatesStaticRow;
    private final Attributes attrs;
    private final boolean hasConditions;
    private final boolean updatesVirtualTables;

    private static final Logger logger = LoggerFactory.getLogger(BatchStatement.class);

    private static final String LOGGED_BATCH_LOW_GCGS_WARNING = "Executing a LOGGED BATCH on table{} {}, configured with a " +
                                                                "gc_grace_seconds of 0. The gc_grace_seconds is used to TTL " +
                                                                "batchlog entries, so setting gc_grace_seconds too low on " +
                                                                "tables involved in an atomic batch might cause batchlog " +
                                                                "entries to expire before being replayed.";

    public static final BatchMetrics metrics = new BatchMetrics();

    /**
     * Creates a new BatchStatement.
     *
     * @param type       type of the batch
     * @param statements the list of statements in the batch
     * @param attrs      additional attributes for statement (CL, timestamp, timeToLive)
     */
    public BatchStatement(String queryString, Type type, VariableSpecifications bindVariables,
                          List<ModificationStatement> statements, Attributes attrs)
    {
        this.rawCQLStatement = queryString;
        this.type = type;
        this.bindVariables = bindVariables;
        this.statements = statements;
        this.attrs = attrs;

        boolean hasConditions = false;
        MultiTableColumnsBuilder regularBuilder = new MultiTableColumnsBuilder();
        RegularAndStaticColumns.Builder conditionBuilder = RegularAndStaticColumns.builder();
        boolean updateRegular = false;
        boolean updateStatic = false;
        boolean updatesVirtualTables = false;

        for (ModificationStatement stmt : statements)
        {
            regularBuilder.addAll(stmt.metadata(), stmt.updatedColumns());
            updateRegular |= stmt.updatesRegularRows();
            updatesVirtualTables |= stmt.isVirtual();
            if (stmt.hasConditions())
            {
                hasConditions = true;
                conditionBuilder.addAll(stmt.conditionColumns());
                updateStatic |= stmt.updatesStaticRow();
            }
        }

        this.updatedColumns = regularBuilder.build();
        this.conditionColumns = conditionBuilder.build();
        this.updatesRegularRows = updateRegular;
        this.updatesStaticRow = updateStatic;
        this.hasConditions = hasConditions;
        this.updatesVirtualTables = updatesVirtualTables;
    }

    @Override
    public String getRawCQLStatement()
    {
        return rawCQLStatement;
    }

    @Override
    public List<ColumnSpecification> getBindVariables()
    {
        return bindVariables.getBindVariables();
    }

    @Override
    public short[] getPartitionKeyBindVariableIndexes()
    {
        boolean affectsMultipleTables =
            !statements.isEmpty() && !statements.stream().map(s -> s.metadata().id).allMatch(isEqual(statements.get(0).metadata().id));

        // Use the TableMetadata of the first statement for partition key bind indexes.  If the statements affect
        // multiple tables, we won't send partition key bind indexes.
        return (affectsMultipleTables || statements.isEmpty())
             ? null
             : bindVariables.getPartitionKeyBindVariableIndexes(statements.get(0).metadata());
    }

    @Override
    public Iterable<org.apache.cassandra.cql3.functions.Function> getFunctions()
    {
        List<org.apache.cassandra.cql3.functions.Function> functions = new ArrayList<>();
        for (ModificationStatement statement : statements)
            statement.addFunctionsTo(functions);
        return functions;
    }

    public void authorize(ClientState state) throws InvalidRequestException, UnauthorizedException
    {
        for (ModificationStatement statement : statements)
            statement.authorize(state);
    }

    // Validates a prepared batch statement without validating its nested statements.
    public void validate() throws InvalidRequestException
    {
        if (attrs.isTimeToLiveSet())
            throw new InvalidRequestException("Global TTL on the BATCH statement is not supported.");

        boolean timestampSet = attrs.isTimestampSet();
        if (timestampSet)
        {
            if (hasConditions)
                throw new InvalidRequestException("Cannot provide custom timestamp for conditional BATCH");

            if (isCounter())
                throw new InvalidRequestException("Cannot provide custom timestamp for counter BATCH");
        }

        boolean hasCounters = false;
        boolean hasNonCounters = false;

        boolean hasVirtualTables = false;
        boolean hasRegularTables = false;

        for (ModificationStatement statement : statements)
        {
            if (timestampSet && statement.isTimestampSet())
                throw new InvalidRequestException("Timestamp must be set either on BATCH or individual statements");

            if (statement.isCounter())
                hasCounters = true;
            else
                hasNonCounters = true;

            if (statement.isVirtual())
                hasVirtualTables = true;
            else
                hasRegularTables = true;
        }

        if (timestampSet && hasCounters)
            throw new InvalidRequestException("Cannot provide custom timestamp for a BATCH containing counters");

        if (isCounter() && hasNonCounters)
            throw new InvalidRequestException("Cannot include non-counter statement in a counter batch");

        if (hasCounters && hasNonCounters)
            throw new InvalidRequestException("Counter and non-counter mutations cannot exist in the same batch");

        if (isLogged() && hasCounters)
            throw new InvalidRequestException("Cannot include a counter statement in a logged batch");

        if (isLogged() && hasVirtualTables)
            throw new InvalidRequestException("Cannot include a virtual table statement in a logged batch");

        if (hasVirtualTables && hasRegularTables)
            throw new InvalidRequestException("Mutations for virtual and regular tables cannot exist in the same batch");

        if (hasConditions && hasVirtualTables)
            throw new InvalidRequestException("Conditional BATCH statements cannot include mutations for virtual tables");

        if (hasConditions)
        {
            String ksName = null;
            String cfName = null;
            for (ModificationStatement stmt : statements)
            {
                if (ksName != null && (!stmt.keyspace().equals(ksName) || !stmt.table().equals(cfName)))
                    throw new InvalidRequestException("Batch with conditions cannot span multiple tables");
                ksName = stmt.keyspace();
                cfName = stmt.table();
            }
        }
    }

    private boolean isCounter()
    {
        return type == Type.COUNTER;
    }

    private boolean isLogged()
    {
        return type == Type.LOGGED;
    }

    // The batch itself will be validated in either Parsed#prepare() - for regular CQL3 batches,
    //   or in QueryProcessor.processBatch() - for native protocol batches.
    @Override
    public void validate(ClientState state) throws InvalidRequestException
    {
        if (isLogged())
            Guardrails.loggedBatchEnabled.ensureEnabled(state);

        for (ModificationStatement statement : statements)
            statement.validate(state);
    }

    public List<ModificationStatement> getStatements()
    {
        return statements;
    }

    @VisibleForTesting
    public List<? extends IMutation> getMutations(ClientState state,
                                                  BatchQueryOptions options,
                                                  boolean local,
                                                  long batchTimestamp,
                                                  long nowInSeconds,
                                                  long queryStartNanoTime)
    {
        if (statements.isEmpty())
            return Collections.emptyList();
        List<List<ByteBuffer>> partitionKeys = new ArrayList<>(statements.size());
        Map<TableId, HashMultiset<ByteBuffer>> partitionCounts = new HashMap<>(updatedColumns.size());
        TableMetadata metadata = statements.get(0).metadata;
        for (int i = 0, isize = statements.size(); i < isize; i++)
        {
            ModificationStatement stmt = statements.get(i);
            if (metadata != null && !stmt.metadata.id.equals(metadata.id))
                metadata = null;
            List<ByteBuffer> stmtPartitionKeys = stmt.buildPartitionKeyNames(options.forStatement(i), state);
            partitionKeys.add(stmtPartitionKeys);
            HashMultiset<ByteBuffer> perKeyCountsForTable = partitionCounts.computeIfAbsent(stmt.metadata.id, k -> HashMultiset.create());
            for (int stmtIdx = 0, stmtSize = stmtPartitionKeys.size(); stmtIdx < stmtSize; stmtIdx++)
                perKeyCountsForTable.add(stmtPartitionKeys.get(stmtIdx));
        }

        Set<String> tablesWithZeroGcGs = null;
        UpdatesCollector collector;
        if (metadata != null)
            collector = new SingleTableUpdatesCollector(metadata, updatedColumns.get(metadata.id), partitionCounts.get(metadata.id));
        else
            collector = new BatchUpdatesCollector(updatedColumns, partitionCounts);

        for (int i = 0, isize = statements.size(); i < isize; i++)
        {
            ModificationStatement statement = statements.get(i);
            if (isLogged() && statement.metadata().params.gcGraceSeconds == 0)
            {
                if (tablesWithZeroGcGs == null)
                    tablesWithZeroGcGs = new HashSet<>();
                tablesWithZeroGcGs.add(statement.metadata.toString());
            }
            QueryOptions statementOptions = options.forStatement(i);
            long timestamp = attrs.getTimestamp(batchTimestamp, statementOptions);
            statement.addUpdates(collector, partitionKeys.get(i), state, statementOptions, local, timestamp, nowInSeconds, queryStartNanoTime);
        }

        if (tablesWithZeroGcGs != null)
        {
            String suffix = tablesWithZeroGcGs.size() == 1 ? "" : "s";
            NoSpamLogger.log(logger, NoSpamLogger.Level.WARN, 1, TimeUnit.MINUTES, LOGGED_BATCH_LOW_GCGS_WARNING,
                             suffix, tablesWithZeroGcGs);
            ClientWarn.instance.warn(MessageFormatter.arrayFormat(LOGGED_BATCH_LOW_GCGS_WARNING, new Object[] { suffix, tablesWithZeroGcGs })
                                                     .getMessage());
        }
        return collector.toMutations();
    }

    /**
     * Checks batch size to ensure threshold is met. If not, a warning is logged.
     *
     * @param mutations - the batch mutations.
     */
    private static void verifyBatchSize(Collection<? extends IMutation> mutations, ClientState clientState) throws InvalidRequestException
    {
        // We only warn for batch spanning multiple mutations (#10876)
        if (mutations.size() <= 1)
            return;

        long size = IMutation.dataSize(mutations);

        if (Guardrails.batchSize.triggersOn(size, clientState))
        {
            Set<String> tableNames = new HashSet<>();
            for (IMutation mutation : mutations)
            {
                for (PartitionUpdate update : mutation.getPartitionUpdates())
                    tableNames.add(update.metadata().toString());
            }

            Guardrails.batchSize.guard(size, tableNames.toString(), false, clientState);
        }
    }

    private void verifyBatchType(Collection<? extends IMutation> mutations, ClientState clientState)
    {
        if (!isLogged() && mutations.size() > 1)
        {
            Set<DecoratedKey> keySet = new HashSet<>();
            Set<String> tableNames = new HashSet<>();

            for (IMutation mutation : mutations)
            {
                for (PartitionUpdate update : mutation.getPartitionUpdates())
                {
                    keySet.add(update.partitionKey());

                    tableNames.add(update.metadata().toString());
                }
            }

            // CASSANDRA-11529: log only if we have more than a threshold of keys, this was also suggested in the
            // original ticket that introduced this warning, CASSANDRA-9282
            if (Guardrails.unloggedBatchAcrossPartitions.triggersOn(keySet.size(), clientState))
            {
                Guardrails.unloggedBatchAcrossPartitions.guard(keySet.size(), tableNames.toString(), false, clientState);
            }
        }
    }


    public ResultMessage execute(QueryState queryState, QueryOptions options, long queryStartNanoTime)
    {
        return execute(queryState, BatchQueryOptions.withoutPerStatementVariables(options), queryStartNanoTime);
    }

    public ResultMessage execute(QueryState queryState, BatchQueryOptions options, long queryStartNanoTime)
    {
        long timestamp = options.getTimestamp(queryState);
        long nowInSeconds = options.getNowInSeconds(queryState);

        ConsistencyLevel cl = options.getConsistency();
        if (cl == null)
            throw new InvalidRequestException("Invalid empty consistency level");

        if (options.getSerialConsistency(queryState) == null)
            throw new InvalidRequestException("Invalid empty serial consistency level");

        ClientState clientState = queryState.getClientState();
        Guardrails.writeConsistencyLevels.guard(EnumSet.of(options.getConsistency(), options.getSerialConsistency(queryState)),
                                                clientState);

        for (int i = 0; i < statements.size(); i++ )
        {
            ModificationStatement statement = statements.get(i);
            statement.validateConsistency(cl, clientState);
            statement.validateDiskUsage(options.forStatement(i), clientState);
        }

        if (hasConditions)
            return executeWithConditions(options, queryState, queryStartNanoTime);

        if (updatesVirtualTables)
            executeInternalWithoutCondition(queryState, options, queryStartNanoTime);
        else
            executeWithoutConditions(getMutations(clientState, options, false, timestamp, nowInSeconds, queryStartNanoTime),
                                     clientState, cl, queryStartNanoTime);

        return new ResultMessage.Void();
    }

    private void executeWithoutConditions(List<? extends IMutation> mutations,
                                          ClientState clientState,
                                          ConsistencyLevel cl,
                                          long queryStartNanoTime) throws RequestExecutionException, RequestValidationException
    {
        if (mutations.isEmpty())
            return;

        verifyBatchSize(mutations, clientState);
        verifyBatchType(mutations, clientState);

        updatePerBatchMetrics(mutations);

        boolean mutateAtomic = (isLogged() && mutations.size() > 1);
        StorageProxy.mutateWithTriggers(mutations, cl, mutateAtomic, queryStartNanoTime, clientState);
        ClientRequestSizeMetrics.recordRowAndColumnCountMetrics(mutations);
    }

    private void updatePerBatchMetrics(Collection<? extends IMutation> mutations)
    {
        int nrUpdatedPartitions = mutations.size();
        int nrUpdatedColumns = 0;
        for (IMutation mutation : mutations)
        {
            for (PartitionUpdate update : mutation.getPartitionUpdates())
            {
                for (Row row : update)
                    nrUpdatedColumns += row.columns().size();
            }
        }
        metrics.update(type, nrUpdatedPartitions, nrUpdatedColumns);
    }

    private ResultMessage executeWithConditions(BatchQueryOptions options, QueryState state, long queryStartNanoTime)
    {
        Pair<CQL3CasRequest, Set<ColumnMetadata>> p = makeCasRequest(options, state);
        CQL3CasRequest casRequest = p.left;
        Set<ColumnMetadata> columnsWithConditions = p.right;

        String ksName = casRequest.metadata.keyspace;
        String tableName = casRequest.metadata.name;

        try (RowIterator result = StorageProxy.cas(ksName,
                                                   tableName,
                                                   casRequest.key,
                                                   casRequest,
                                                   options.getSerialConsistency(state),
                                                   options.getConsistency(),
                                                   state.getClientState(),
                                                   options.getNowInSeconds(state),
                                                   queryStartNanoTime))
        {
            return new ResultMessage.Rows(ModificationStatement.buildCasResultSet(ksName,
                                                                                  tableName,
                                                                                  result,
                                                                                  columnsWithConditions,
                                                                                  true,
                                                                                  state,
                                                                                  options.forStatement(0)));
        }
    }

    private Pair<CQL3CasRequest,Set<ColumnMetadata>> makeCasRequest(BatchQueryOptions options, QueryState state)
    {
        long batchTimestamp = options.getTimestamp(state);
        long nowInSeconds = options.getNowInSeconds(state);
        DecoratedKey key = null;
        CQL3CasRequest casRequest = null;
        Set<ColumnMetadata> columnsWithConditions = new LinkedHashSet<>();

        for (int i = 0; i < statements.size(); i++)
        {
            ModificationStatement statement = statements.get(i);
            QueryOptions statementOptions = options.forStatement(i);
            long timestamp = attrs.getTimestamp(batchTimestamp, statementOptions);
            List<ByteBuffer> pks = statement.buildPartitionKeyNames(statementOptions, state.getClientState());
            if (statement.getRestrictions().keyIsInRelation())
                throw new IllegalArgumentException("Batch with conditions cannot span multiple partitions (you cannot use IN on the partition key)");
            if (key == null)
            {
                key = statement.metadata().partitioner.decorateKey(pks.get(0));
                casRequest = new CQL3CasRequest(statement.metadata(), key, conditionColumns, updatesRegularRows, updatesStaticRow);
            }
            else if (!key.getKey().equals(pks.get(0)))
            {
                throw new InvalidRequestException("Batch with conditions cannot span multiple partitions");
            }

            checkFalse(statement.getRestrictions().clusteringKeyRestrictionsHasIN(),
                       "IN on the clustering key columns is not supported with conditional %s",
                       statement.type.isUpdate()? "updates" : "deletions");

            if (statement.hasSlices())
            {
                // All of the conditions require meaningful Clustering, not Slices
                assert !statement.hasConditions();

                Slices slices = statement.createSlices(statementOptions);
                // If all the ranges were invalid we do not need to do anything.
                if (slices.isEmpty())
                    continue;

                for (Slice slice : slices)
                {
                    casRequest.addRangeDeletion(slice, statement, statementOptions, timestamp, nowInSeconds);
                }

            }
            else
            {
                Clustering<?> clustering = Iterables.getOnlyElement(statement.createClustering(statementOptions, state.getClientState()));
                if (statement.hasConditions())
                {
                    statement.addConditions(clustering, casRequest, statementOptions);
                    // As soon as we have a ifNotExists, we set columnsWithConditions to null so that everything is in the resultSet
                    if (statement.hasIfNotExistCondition() || statement.hasIfExistCondition())
                        columnsWithConditions = null;
                    else if (columnsWithConditions != null)
                        Iterables.addAll(columnsWithConditions, statement.getColumnsWithConditions());
                }
                casRequest.addRowUpdate(clustering, statement, statementOptions, timestamp, nowInSeconds);
            }
        }

        return Pair.create(casRequest, columnsWithConditions);
    }

    public boolean hasConditions()
    {
        return hasConditions;
    }

    public ResultMessage executeLocally(QueryState queryState, QueryOptions options) throws RequestValidationException, RequestExecutionException
    {
        BatchQueryOptions batchOptions = BatchQueryOptions.withoutPerStatementVariables(options);

        if (hasConditions)
            return executeInternalWithConditions(batchOptions, queryState);

        executeInternalWithoutCondition(queryState, batchOptions, nanoTime());
        return new ResultMessage.Void();
    }

    private ResultMessage executeInternalWithoutCondition(QueryState queryState, BatchQueryOptions batchOptions, long queryStartNanoTime)
    {
        long timestamp = batchOptions.getTimestamp(queryState);
        long nowInSeconds = batchOptions.getNowInSeconds(queryState);

        for (IMutation mutation : getMutations(queryState.getClientState(), batchOptions, true, timestamp, nowInSeconds, queryStartNanoTime))
            mutation.apply();
        return null;
    }

    private ResultMessage executeInternalWithConditions(BatchQueryOptions options, QueryState state)
    {
        Pair<CQL3CasRequest, Set<ColumnMetadata>> p = makeCasRequest(options, state);
        CQL3CasRequest request = p.left;
        Set<ColumnMetadata> columnsWithConditions = p.right;

        String ksName = request.metadata.keyspace;
        String tableName = request.metadata.name;

        long timestamp = options.getTimestamp(state);
        long nowInSeconds = options.getNowInSeconds(state);

        try (RowIterator result = ModificationStatement.casInternal(state.getClientState(), request, timestamp, nowInSeconds))
        {
            ResultSet resultSet =
                ModificationStatement.buildCasResultSet(ksName,
                                                        tableName,
                                                        result,
                                                        columnsWithConditions,
                                                        true,
                                                        state,
                                                        options.forStatement(0));
            return new ResultMessage.Rows(resultSet);
        }
    }

    public String toString()
    {
        return String.format("BatchStatement(type=%s, statements=%s)", type, statements);
    }

    public static class Parsed extends RawKeyspaceAwareStatement<BatchStatement>
    {
        private final Type type;
        private final Attributes.Raw attrs;
        private final List<ModificationStatement.Parsed> parsedStatements;

        public Parsed(Type type, Attributes.Raw attrs, List<ModificationStatement.Parsed> parsedStatements)
        {
            this.type = type;
            this.attrs = attrs;
            this.parsedStatements = parsedStatements;
        }

        private void setKeyspace(ClientState state) throws InvalidRequestException
        {
            for (ModificationStatement.Parsed statement : parsedStatements)
                statement.setKeyspace(state);
        }

        public void setKeyspace(Function<ModificationStatement.Parsed, String> convertKeyspace) throws InvalidRequestException
        {
            for (ModificationStatement.Parsed statement : parsedStatements)
                statement.setKeyspace(convertKeyspace.apply(statement));
        }

        @Override
        public BatchStatement prepare(ClientState state, UnaryOperator<String> keyspaceMapper)
        {
            setKeyspace(state);

            List<ModificationStatement> statements = new ArrayList<>(parsedStatements.size());
            parsedStatements.forEach(s -> statements.add(s.prepare(state, bindVariables, keyspaceMapper)));

            Attributes prepAttrs = attrs.prepare("[batch]", "[batch]");
            prepAttrs.collectMarkerSpecification(bindVariables);

            BatchStatement batchStatement = new BatchStatement(rawCQLStatement, type, bindVariables, statements, prepAttrs);
            batchStatement.validate();

            return batchStatement;
        }
    }

    private static class MultiTableColumnsBuilder
    {
        private final Map<TableId, RegularAndStaticColumns.Builder> perTableBuilders = new HashMap<>();

        public void addAll(TableMetadata table, RegularAndStaticColumns columns)
        {
            RegularAndStaticColumns.Builder builder = perTableBuilders.get(table.id);
            if (builder == null)
            {
                builder = RegularAndStaticColumns.builder();
                perTableBuilders.put(table.id, builder);
            }
            builder.addAll(columns);
        }

        public Map<TableId, RegularAndStaticColumns> build()
        {
            Map<TableId, RegularAndStaticColumns> m = Maps.newHashMapWithExpectedSize(perTableBuilders.size());
            for (Map.Entry<TableId, RegularAndStaticColumns.Builder> p : perTableBuilders.entrySet())
                m.put(p.getKey(), p.getValue().build());
            return m;
        }
    }

    @Override
    public AuditLogContext getAuditLogContext()
    {
        return new AuditLogContext(AuditLogEntryType.BATCH);
    }
}

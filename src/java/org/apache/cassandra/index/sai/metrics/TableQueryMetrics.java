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
package org.apache.cassandra.index.sai.metrics;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Timer;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.tracing.Tracing;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

public class TableQueryMetrics extends AbstractMetrics
{
    public static final String TABLE_QUERY_METRIC_TYPE = "TableQueryMetrics";

    public final Timer postFilteringReadLatency;

    private final PerQueryMetrics perQueryMetrics;

    private final Counter totalQueryTimeouts;
    private final Counter totalPartitionReads;
    private final Counter totalRowsFiltered;
    private final Counter totalQueriesCompleted;

    private final Counter sortThenFilterQueriesCompleted;
    private final Counter filterThenSortQueriesCompleted;


    public TableQueryMetrics(TableMetadata table)
    {
        super(table.keyspace, table.name, TABLE_QUERY_METRIC_TYPE);

        perQueryMetrics = new PerQueryMetrics(table);

        postFilteringReadLatency = Metrics.timer(createMetricName("PostFilteringReadLatency"));

        totalPartitionReads = Metrics.counter(createMetricName("TotalPartitionReads"));
        totalRowsFiltered = Metrics.counter(createMetricName("TotalRowsFiltered"));
        totalQueriesCompleted = Metrics.counter(createMetricName("TotalQueriesCompleted"));
        totalQueryTimeouts = Metrics.counter(createMetricName("TotalQueryTimeouts"));

        sortThenFilterQueriesCompleted = Metrics.counter(createMetricName("SortThenFilterQueriesCompleted"));
        filterThenSortQueriesCompleted = Metrics.counter(createMetricName("FilterThenSortQueriesCompleted"));
    }

    public void record(QueryContext queryContext)
    {
        if (queryContext.queryTimeouts() > 0)
        {
            assert queryContext.queryTimeouts() == 1;

            totalQueryTimeouts.inc();
        }

        perQueryMetrics.record(queryContext);
    }

    public void release()
    {
        super.release();
        perQueryMetrics.release();
    }

    public class PerQueryMetrics extends AbstractMetrics
    {
        private final Timer queryLatency;

        /**
         * Global metrics for all indices hit during the query.
         */
        private final Histogram sstablesHit;
        private final Histogram segmentsHit;
        private final Histogram partitionReads;
        private final Histogram rowsFiltered;

        /**
         * BKD index metrics.
         */
        private final Histogram kdTreePostingsNumPostings;
        /**
         * BKD index posting lists metrics.
         */
        private final Histogram kdTreePostingsSkips;
        private final Histogram kdTreePostingsDecodes;

        /** Shadowed keys scan metrics **/
        private final Histogram shadowedKeysScannedHistogram;

        /**
         * Trie index posting lists metrics.
         */
        private final Histogram postingsSkips;
        private final Histogram postingsDecodes;

        private final LongAdder annNodesVisited = new LongAdder();

        public PerQueryMetrics(TableMetadata table)
        {
            super(table.keyspace, table.name, "PerQuery");

            queryLatency = Metrics.timer(createMetricName("QueryLatency"));

            sstablesHit = Metrics.histogram(createMetricName("SSTableIndexesHit"), false);
            segmentsHit = Metrics.histogram(createMetricName("IndexSegmentsHit"), false);

            kdTreePostingsSkips = Metrics.histogram(createMetricName("KDTreePostingsSkips"), false);

            kdTreePostingsNumPostings = Metrics.histogram(createMetricName("KDTreePostingsNumPostings"), false);
            kdTreePostingsDecodes = Metrics.histogram(createMetricName("KDTreePostingsDecodes"), false);

            postingsSkips = Metrics.histogram(createMetricName("PostingsSkips"), false);
            postingsDecodes = Metrics.histogram(createMetricName("PostingsDecodes"), false);

            partitionReads = Metrics.histogram(createMetricName("PartitionReads"), false);
            rowsFiltered = Metrics.histogram(createMetricName("RowsFiltered"), false);

            shadowedKeysScannedHistogram = Metrics.histogram(createMetricName("ShadowedKeysScannedHistogram"), false);
        }

        private void recordStringIndexCacheMetrics(QueryContext events)
        {
            postingsSkips.update(events.triePostingsSkips());
            postingsDecodes.update(events.triePostingsDecodes());
        }

        private void recordNumericIndexCacheMetrics(QueryContext events)
        {
            kdTreePostingsNumPostings.update(events.bkdPostingListsHit());

            kdTreePostingsSkips.update(events.bkdPostingsSkips());
            kdTreePostingsDecodes.update(events.bkdPostingsDecodes());
        }

        private void recordAnnIndexMetrics(QueryContext queryContext)
        {
            annNodesVisited.add(queryContext.annNodesVisited());
        }

        public void record(QueryContext queryContext)
        {
            final long totalQueryTimeNs = queryContext.totalQueryTimeNs();
            queryLatency.update(totalQueryTimeNs, TimeUnit.NANOSECONDS);
            final long queryLatencyMicros = TimeUnit.NANOSECONDS.toMicros(totalQueryTimeNs);

            final long ssTablesHit = queryContext.sstablesHit();
            final long segmentsHit = queryContext.segmentsHit();
            final long partitionsRead = queryContext.partitionsRead();
            final long rowsFiltered = queryContext.rowsFiltered();
            final long rowsPreFiltered = queryContext.rowsFiltered();

            sstablesHit.update(ssTablesHit);
            this.segmentsHit.update(segmentsHit);

            partitionReads.update(partitionsRead);
            totalPartitionReads.inc(partitionsRead);

            this.rowsFiltered.update(rowsFiltered);
            totalRowsFiltered.inc(rowsFiltered);

            if (queryContext.filterSortOrder() == QueryContext.FilterSortOrder.SORT_THEN_FILTER)
                sortThenFilterQueriesCompleted.inc();
            else if (queryContext.filterSortOrder() == QueryContext.FilterSortOrder.FILTER_THEN_SORT)
                filterThenSortQueriesCompleted.inc();

            if (Tracing.isTracing())
            {
                if (queryContext.filterSortOrder() == QueryContext.FilterSortOrder.FILTER_THEN_SORT)
                {
                    Tracing.trace("Index query accessed memtable indexes, {}, and {}, selected {} before ranking, post-filtered {} in {}, and took {} microseconds.",
                                  pluralize(ssTablesHit, "SSTable index", "es"),
                                  pluralize(segmentsHit, "segment", "s"),
                                  pluralize(rowsPreFiltered, "row", "s"),
                                  pluralize(rowsFiltered, "row", "s"),
                                  pluralize(partitionsRead, "partition", "s"),
                                  queryLatencyMicros);
                }
                else
                {
                    Tracing.trace("Index query accessed memtable indexes, {}, and {}, post-filtered {} in {}, and took {} microseconds.",
                                  pluralize(ssTablesHit, "SSTable index", "es"),
                                  pluralize(segmentsHit, "segment", "s"),
                                  pluralize(rowsFiltered, "row", "s"),
                                  pluralize(partitionsRead, "partition", "s"),
                                  queryLatencyMicros);
                }
            }

            if (queryContext.trieSegmentsHit() > 0)
                recordStringIndexCacheMetrics(queryContext);
            if (queryContext.bkdSegmentsHit() > 0)
                recordNumericIndexCacheMetrics(queryContext);
            if (queryContext.annNodesVisited() > 0)
                recordAnnIndexMetrics(queryContext);

            shadowedKeysScannedHistogram.update(queryContext.getShadowedPrimaryKeyCount());

            totalQueriesCompleted.inc();
        }
    }

    private String pluralize(long count, String root, String plural)
    {
        return count == 1 ? String.format("1 %s", root) : String.format("%d %s%s", count, root, plural);
    }
}

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
package org.apache.cassandra.tools.nodetool;

import java.io.PrintStream;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import io.airlift.airline.Command;
import io.airlift.airline.Option;

import org.apache.cassandra.db.compaction.CompactionStrategyStatistics;
import org.apache.cassandra.db.compaction.TableOperation;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.metrics.CassandraMetricsRegistry;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;
import org.apache.cassandra.tools.nodetool.formatter.TableBuilder;

import static java.lang.String.format;

@Command(name = "compactionstats", description = "Print statistics on compactions")
public class CompactionStats extends NodeToolCmd
{
    @Option(title = "human_readable",
            name = {"-H", "--human-readable"},
            description = "Display bytes in human readable form, i.e. KiB, MiB, GiB, TiB")
    private boolean humanReadable = false;

    @Option(title = "vtable_output",
            name = {"-V", "--vtable"},
            description = "Display fields matching vtable output")
    private boolean vtableOutput = false;

    @Option(title = "aggregate",
    name = {"-A", "--aggregate"},
    description = "Show the compaction aggregates for the compactions in progress, e.g. the levels for LCS or the buckets for STCS and TWCS.")
    private boolean aggregate = false;

    @Override
    public void execute(NodeProbe probe)
    {
        PrintStream out = probe.output().out;
        TableBuilder tableBuilder = new TableBuilder();
        pendingTasksAndConcurrentCompactorsStats(probe, tableBuilder);
        compactionsStats(probe, tableBuilder);
        reportCompactionTable(probe.getCompactionManagerProxy().getCompactions(), probe.getCompactionThroughputBytes(), humanReadable, vtableOutput, out, tableBuilder);

        if (aggregate)
        {
            reportAggregateCompactions(probe);
        }
    }

    private void pendingTasksAndConcurrentCompactorsStats(NodeProbe probe, TableBuilder tableBuilder)
    {
        Map<String, Map<String, Integer>> pendingTaskNumberByTable =
            (Map<String, Map<String, Integer>>) probe.getCompactionMetric("PendingTasksByTableName");
        Map<String, Map<String, Double>> writeAmplificationByTableName =
            (Map<String, Map<String, Double>>) probe.getCompactionMetric("WriteAmplificationByTableName");

        tableBuilder.add("concurrent compactors", Integer.toString(probe.getConcurrentCompactors()));
        int numTotalPendingTasks = 0;
        double totWriteAmplification = 0;
        for (Entry<String, Map<String, Integer>> ksEntry : pendingTaskNumberByTable.entrySet())
        {
            Map<String, Double> ksWriteAmplification = writeAmplificationByTableName.get(ksEntry.getKey());
            for (Entry<String, Integer> tableEntry : ksEntry.getValue().entrySet())
            {
                numTotalPendingTasks += tableEntry.getValue();
                if (ksWriteAmplification != null)
                    totWriteAmplification += ksWriteAmplification.get(tableEntry.getKey());
            }
        }
        tableBuilder.add("pending tasks", Integer.toString(numTotalPendingTasks));
        tableBuilder.add("write amplification", String.format("%.2f", totWriteAmplification));

        for (Entry<String, Map<String, Integer>> ksEntry : pendingTaskNumberByTable.entrySet())
        {
            Map<String, Double> ksWriteAmplification = writeAmplificationByTableName.get(ksEntry.getKey());
            for (Entry<String, Integer> tableEntry : ksEntry.getValue().entrySet())
            {
                double wa = ksWriteAmplification == null ? 0 : ksWriteAmplification.get(tableEntry.getKey());
                tableBuilder.add(ksEntry.getKey(), tableEntry.getKey(), tableEntry.getValue().toString());
                tableBuilder.add(ksEntry.getKey(), String.format("%s write amplification", tableEntry.getKey()), String.format("%.2f", wa));
            }
        }
    }

    private void compactionsStats(NodeProbe probe, TableBuilder tableBuilder)
    {
        CassandraMetricsRegistry.JmxMeterMBean totalCompactionsCompletedMetrics =
        (CassandraMetricsRegistry.JmxMeterMBean) probe.getCompactionMetric("TotalCompactionsCompleted");
        tableBuilder.add("compactions completed", String.valueOf(totalCompactionsCompletedMetrics.getCount()));

        CassandraMetricsRegistry.JmxCounterMBean bytesCompacted = (CassandraMetricsRegistry.JmxCounterMBean) probe.getCompactionMetric("BytesCompacted");
        if (humanReadable)
            tableBuilder.add("data compacted", FileUtils.stringifyFileSize(Double.parseDouble(Long.toString(bytesCompacted.getCount()))));
        else
            tableBuilder.add("data compacted", Long.toString(bytesCompacted.getCount()));

        CassandraMetricsRegistry.JmxCounterMBean compactionsAborted = (CassandraMetricsRegistry.JmxCounterMBean) probe.getCompactionMetric("CompactionsAborted");
        tableBuilder.add("compactions aborted", Long.toString(compactionsAborted.getCount()));

        CassandraMetricsRegistry.JmxCounterMBean compactionsReduced = (CassandraMetricsRegistry.JmxCounterMBean) probe.getCompactionMetric("CompactionsReduced");
        tableBuilder.add("compactions reduced", Long.toString(compactionsReduced.getCount()));

        CassandraMetricsRegistry.JmxCounterMBean sstablesDroppedFromCompaction = (CassandraMetricsRegistry.JmxCounterMBean) probe.getCompactionMetric("SSTablesDroppedFromCompaction");
        tableBuilder.add("sstables dropped from compaction", Long.toString(sstablesDroppedFromCompaction.getCount()));

        NumberFormat formatter = new DecimalFormat("0.00");

        tableBuilder.add("15 minute rate", String.format("%s/minute", formatter.format(totalCompactionsCompletedMetrics.getFifteenMinuteRate() * 60)));
        tableBuilder.add("mean rate", String.format("%s/hour", formatter.format(totalCompactionsCompletedMetrics.getMeanRate() * 60 * 60)));

        double configured = probe.getStorageService().getCompactionThroughtputMibPerSecAsDouble();
        tableBuilder.add("compaction throughput (MiB/s)", configured == 0 ? "throttling disabled (0)" : Double.toString(configured));
    }

    public static void reportCompactionTable(List<Map<String,String>> compactions, long compactionThroughputInBytes, boolean humanReadable, PrintStream out, TableBuilder table)
    {
        reportCompactionTable(compactions, compactionThroughputInBytes, humanReadable, false, out, table);
    }

    public static void reportCompactionTable(List<Map<String,String>> compactions, long compactionThroughputInBytes, boolean humanReadable, boolean vtableOutput, PrintStream out, TableBuilder table)
    {
        if (compactions.isEmpty())
        {
            table.printTo(out);
            return;
        }

        long remainingBytes = 0;

        if (vtableOutput)
            table.add("keyspace", "table", "task id", "completion ratio", "kind", "progress", "sstables", "total", "unit", "target directory");
        else
            table.add("id", "compaction type", "keyspace", "table", "completed", "total", "unit", "progress");

        for (Map<String, String> c : compactions)
        {
            long total = Long.parseLong(c.get(TableOperation.Progress.TOTAL));
            long completed = Long.parseLong(c.get(TableOperation.Progress.COMPLETED));
            String taskType = c.get(TableOperation.Progress.OPERATION_TYPE);
            String keyspace = c.get(TableOperation.Progress.KEYSPACE);
            String columnFamily = c.get(TableOperation.Progress.COLUMNFAMILY);
            String unit = c.get(TableOperation.Progress.UNIT);
            boolean toFileSize = humanReadable && TableOperation.Unit.isFileSize(unit);
            String[] tables = c.get(TableOperation.Progress.SSTABLES).split(",");
            String progressStr = toFileSize ? FileUtils.stringifyFileSize(completed) : Long.toString(completed);
            String totalStr = toFileSize ? FileUtils.stringifyFileSize(total) : Long.toString(total);
            String percentComplete = total == 0 ? "n/a" : new DecimalFormat("0.00").format((double) completed / total * 100) + '%';
            String id = c.get(TableOperation.Progress.OPERATION_ID);
            if (vtableOutput)
            {
                String targetDirectory = c.get(TableOperation.Progress.TARGET_DIRECTORY);
                table.add(keyspace, columnFamily, id, percentComplete, taskType, progressStr, String.valueOf(tables.length), totalStr, unit, targetDirectory);
            }
            else
                table.add(id, taskType, keyspace, columnFamily, progressStr, totalStr, unit, percentComplete);

            remainingBytes += total - completed;
        }

        String remainingTime = "n/a";
        if (compactionThroughputInBytes != 0)
        {
            long remainingTimeInSecs = remainingBytes / compactionThroughputInBytes;
            remainingTime = format("%dh%02dm%02ds", remainingTimeInSecs / 3600, (remainingTimeInSecs % 3600) / 60, (remainingTimeInSecs % 60));
        }

        table.add("active compaction remaining time", remainingTime);
        table.printTo(out);
    }

    private static void reportAggregateCompactions(NodeProbe probe)
    {
        List<CompactionStrategyStatistics> statistics = (List<CompactionStrategyStatistics>) probe.getCompactionMetric("AggregateCompactions");
        if (statistics.isEmpty())
            return;

        System.out.println("Aggregated view:");
        for (CompactionStrategyStatistics stat : statistics)
            System.out.println(stat.toString());
    }
}
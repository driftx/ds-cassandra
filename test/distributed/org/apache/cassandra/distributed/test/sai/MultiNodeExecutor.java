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

package org.apache.cassandra.distributed.test.sai;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.util.ColumnTypeUtil;
import org.apache.cassandra.index.sai.cql.DataModel;

public class MultiNodeExecutor implements DataModel.Executor
{
    private final Cluster cluster;

    public MultiNodeExecutor(Cluster cluster)
    {
        this.cluster = cluster;
    }

    @Override
    public void createTable(String statement)
    {
        cluster.schemaChange(statement);
    }

    @Override
    public void flush(String keyspace, String table)
    {
        cluster.forEach(node -> node.flush(keyspace));
    }

    @Override
    public void compact(String keyspace, String table)
    {
        cluster.forEach(node -> node.forceCompact(keyspace, table));
    }

    @Override
    public void disableCompaction(String keyspace, String table)
    {
        cluster.forEach((node) -> node.runOnInstance(() -> Keyspace.open(keyspace).getColumnFamilyStore(table).disableAutoCompaction()));
    }

    @Override
    public void waitForIndexQueryable(String keyspace, String table)
    {
        SAIUtil.waitForIndexQueryable(cluster, keyspace);
    }

    @Override
    public void executeLocal(String query, Object... values) throws Throwable
    {
        Object[] buffers = ColumnTypeUtil.transformValues(values);
        cluster.coordinator(1).execute(query, ConsistencyLevel.QUORUM, buffers);
    }

    @Override
    public List<Object> executeRemote(String query, int fetchSize, Object... values) throws Throwable
    {
        Object[] buffers = ColumnTypeUtil.transformValues(values);
        Iterator<Object> iterator = cluster.coordinator(1).executeWithPagingWithResult(query, ConsistencyLevel.QUORUM, fetchSize, buffers).map(row -> row.get(0));

        List<Object> result = new ArrayList<>();
        iterator.forEachRemaining(result::add);

        return result;
    }

    @Override
    public void counterReset()
    {
        AbstractQueryTester.Counter.reset();
    }

    @Override
    public long getCounter()
    {
        return AbstractQueryTester.Counter.get();
    }
}

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
package org.apache.cassandra.index.sai.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.SSTableIndex;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.Throwables;

/**
 * TermIterator wraps RangeUnionIterator with code that tracks and releases the referenced indexes,
 * and adds timeout checkpoints around expensive operations.
 */
public class TermIterator extends RangeIterator
{
    private static final Logger logger = LoggerFactory.getLogger(TermIterator.class);

    private final QueryContext context;

    private final RangeIterator union;
    private final Set<SSTableIndex> referencedIndexes;

    private TermIterator(RangeIterator union, Set<SSTableIndex> referencedIndexes, QueryContext queryContext)
    {
        super(union.getMinimum(), union.getMaximum(), union.getMaxKeys());

        this.union = union;
        this.referencedIndexes = referencedIndexes;
        this.context = queryContext;
    }


    @SuppressWarnings("resource")
    public static TermIterator build(final Expression e, Set<SSTableIndex> perSSTableIndexes, AbstractBounds<PartitionPosition> keyRange, QueryContext queryContext, boolean defer, int limit)
    {
        RangeIterator rangeIterator = buildRangeIterator(e, perSSTableIndexes, keyRange, queryContext, defer, limit);
        return new TermIterator(rangeIterator, perSSTableIndexes, queryContext);
    }

    private static RangeIterator buildRangeIterator(final Expression e, Set<SSTableIndex> perSSTableIndexes, AbstractBounds<PartitionPosition> keyRange, QueryContext queryContext, boolean defer, int limit)
    {
        final List<RangeIterator> tokens = new ArrayList<>(1 + perSSTableIndexes.size());

        RangeIterator memtableIterator = e.context.searchMemtable(queryContext, e, keyRange, limit);
        if (memtableIterator != null)
            tokens.add(memtableIterator);

        for (final SSTableIndex index : perSSTableIndexes)
        {
            try
            {
                queryContext.checkpoint();
                queryContext.addSstablesHit(1);
                assert !index.isReleased();



                RangeIterator keyIterator = index.search(e, keyRange, queryContext, defer, limit);

                if (keyIterator == null || !keyIterator.hasNext())
                    continue;

                tokens.add(keyIterator);
            }
            catch (Throwable e1)
            {
                if (logger.isDebugEnabled() && !(e1 instanceof AbortedOperationException))
                    logger.debug(String.format("Failed search an index %s, skipping.", index.getSSTable()), e1);

                // Close the iterators that were successfully opened before the error
                FileUtils.closeQuietly(tokens);

                throw Throwables.cleaned(e1);
            }
        }

        return RangeUnionIterator.build(tokens);
    }

    protected PrimaryKey computeNext()
    {
        try
        {
            return union.hasNext() ? union.next() : endOfData();
        }
        finally
        {
            context.checkpoint();
        }
    }

    protected void performSkipTo(PrimaryKey nextKey)
    {
        try
        {
            union.skipTo(nextKey);
        }
        finally
        {
            context.checkpoint();
        }
    }

    public void close()
    {
        FileUtils.closeQuietly(union);
        referencedIndexes.forEach(TermIterator::releaseQuietly);
        referencedIndexes.clear();
    }

    private static void releaseQuietly(SSTableIndex index)
    {
        try
        {
            index.release();
        }
        catch (Throwable e)
        {
            logger.error(String.format("Failed to release index %s", index.getSSTable()), e);
        }
    }
}

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

package org.apache.cassandra.index.sai.memory;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.memtable.AbstractAllocatorMemtable;
import org.apache.cassandra.db.memtable.AbstractShardedMemtable;
import org.apache.cassandra.db.memtable.TrieMemtable;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.BootStrapper;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.ExcludingBounds;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.IncludingExcludingBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.RangeIterator;
import org.apache.cassandra.inject.Injections;
import org.apache.cassandra.inject.InvokePointBuilder;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.schema.MockSchema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.cassandra.utils.concurrent.OpOrder;

import static org.apache.cassandra.config.CassandraRelevantProperties.MEMTABLE_SHARD_COUNT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TrieMemtableIndexTest extends SAITester
{
    private static final Injections.Counter indexSearchCounter = Injections.newCounter("IndexSearchCounter")
                                                                           .add(InvokePointBuilder.newInvokePoint()
                                                                                                  .onClass(TrieMemoryIndex.class)
                                                                                                  .onMethod("search"))
                                                                           .build();

    private ColumnFamilyStore cfs;
    private IndexContext indexContext;
    private TrieMemtableIndex memtableIndex;
    private AbstractAllocatorMemtable memtable;
    private IPartitioner partitioner;
    private Map<DecoratedKey, Integer> keyMap;
    private Map<Integer, Integer> rowMap;

    @BeforeClass
    public static void setShardCount()
    {
        MEMTABLE_SHARD_COUNT.setInt(8);
    }

    @Before
    public void setup() throws Throwable
    {
        assertEquals(8, AbstractShardedMemtable.getDefaultShardCount());

        TokenMetadata metadata = StorageService.instance.getTokenMetadata();
        metadata.updateNormalTokens(BootStrapper.getRandomTokens(metadata, 10), FBUtilities.getBroadcastAddressAndPort());

        TableMetadata tableMetadata = TableMetadata.builder("ks", "tb")
                                                   .addPartitionKeyColumn("pk", Int32Type.instance)
                                                   .addRegularColumn("val", Int32Type.instance)
                                                   .build();
        cfs = MockSchema.newCFS(tableMetadata);
        partitioner = cfs.getPartitioner();
        memtable = (AbstractAllocatorMemtable) cfs.getCurrentMemtable();
        indexContext = SAITester.createIndexContext("index", Int32Type.instance, cfs);
        indexSearchCounter.reset();
        keyMap = new TreeMap<>();
        rowMap = new HashMap<>();

        Injections.inject(indexSearchCounter);
    }

    @After
    public void resetBufferType() throws Exception
    {
        setTrieMemtableBufferType(BufferType.OFF_HEAP);
    }

    @Test
    public void onHeapAllocation() throws Exception
    {
        setTrieMemtableBufferType(BufferType.ON_HEAP);
        memtableIndex = new TrieMemtableIndex(indexContext, memtable);
        assertEquals(AbstractShardedMemtable.getDefaultShardCount(), memtableIndex.shardCount());

        assertTrue(memtable.getAllocator().onHeap().owns() == 0);
        assertTrue(memtable.getAllocator().offHeap().owns() == 0);

        for (int row = 0; row < 100; row++)
        {
            addRow(row, row);
        }

        assertTrue(memtable.getAllocator().onHeap().owns() > 0);
        assertTrue(memtable.getAllocator().offHeap().owns() == 0);
    }

    @Test
    public void offHeapAllocation() throws Exception
    {
        setTrieMemtableBufferType(BufferType.OFF_HEAP);
        memtableIndex = new TrieMemtableIndex(indexContext, memtable);
        assertEquals(AbstractShardedMemtable.getDefaultShardCount(), memtableIndex.shardCount());

        assertTrue(memtable.getAllocator().onHeap().owns() == 0);
        assertTrue(memtable.getAllocator().offHeap().owns() == 0);

        for (int row = 0; row < 100; row++)
        {
            addRow(row, row);
        }

        assertTrue(memtable.getAllocator().onHeap().owns() > 0);
        assertTrue(memtable.getAllocator().offHeap().owns() > 0);
    }

    @Test
    public void randomQueryTest() throws Exception
    {
        memtableIndex = new TrieMemtableIndex(indexContext, memtable);
        assertEquals(AbstractShardedMemtable.getDefaultShardCount(), memtableIndex.shardCount());

        for (int row = 0; row < getRandom().nextIntBetween(1000, 5000); row++)
        {
            int pk = getRandom().nextIntBetween(0, 10000);
            while (rowMap.containsKey(pk))
                pk = getRandom().nextIntBetween(0, 10000);
            int value = getRandom().nextIntBetween(0, 100);
            rowMap.put(pk, value);
            addRow(pk, value);
        }

        List<DecoratedKey> keys = new ArrayList<>(keyMap.keySet());

        for (int executionCount = 0; executionCount < 1000; executionCount++)
        {
            Expression expression = generateRandomExpression();

            AbstractBounds<PartitionPosition> keyRange = generateRandomBounds(keys);

            Set<Integer> expectedKeys = keyMap.keySet()
                                              .stream()
                                              .filter(keyRange::contains)
                                              .map(keyMap::get)
                                              .filter(pk -> expression.isSatisfiedBy(Int32Type.instance.decompose(rowMap.get(pk))))
                                              .collect(Collectors.toSet());

            Set<Integer> foundKeys = new HashSet<>();

            try (RangeIterator iterator = memtableIndex.search(new QueryContext(), expression, keyRange, 0))
            {
                while (iterator.hasNext())
                {
                    DecoratedKey k = iterator.next().partitionKey();
                    int key = Int32Type.instance.compose(k.getKey());
                    assertFalse(foundKeys.contains(key));
                    foundKeys.add(key);
                }
            }

            assertEquals(expectedKeys, foundKeys);
        }
    }

    @Test
    public void indexIteratorTest()
    {
        memtableIndex = new TrieMemtableIndex(indexContext, memtable);

        Map<Integer, Set<DecoratedKey>> terms = buildTermMap();

        terms.entrySet()
             .stream()
             .forEach(entry -> entry.getValue()
                                    .forEach(pk -> addRow(Int32Type.instance.compose(pk.getKey()), entry.getKey())));

        for (int executionCount = 0; executionCount < 1000; executionCount++)
        {
            // These keys have midrange tokens that select 3 of the 8 range indexes
            DecoratedKey temp1 = makeKey(cfs.metadata(), getRandom().nextIntBetween(0, 20000));
            DecoratedKey temp2 = makeKey(cfs.metadata(), getRandom().nextIntBetween(0, 20000));
            DecoratedKey minimum = temp1.compareTo(temp2) <= 0 ? temp1 : temp2;
            DecoratedKey maximum = temp1.compareTo(temp2) <= 0 ? temp2 : temp1;

            Iterator<Pair<ByteComparable, Iterator<PrimaryKey>>> iterator = memtableIndex.iterator(minimum, maximum);

            while (iterator.hasNext())
            {
                Pair<ByteComparable, Iterator<PrimaryKey>> termPair = iterator.next();
                int term = termFromComparable(termPair.left);
                // The iterator will return keys outside the range of min/max so we need to filter here to
                // get the correct keys
                List<DecoratedKey> expectedPks = terms.get(term)
                                                      .stream()
                                                      .filter(pk -> pk.compareTo(minimum) >= 0 && pk.compareTo(maximum) <= 0)
                                                      .sorted()
                                                      .collect(Collectors.toList());
                List<DecoratedKey> termPks = new ArrayList<>();
                while (termPair.right.hasNext())
                {
                    DecoratedKey pk = termPair.right.next().partitionKey();
                    if (pk.compareTo(minimum) >= 0 && pk.compareTo(maximum) <= 0)
                        termPks.add(pk);
                }
                assertEquals(expectedPks, termPks);
            }
        }
    }

    private Expression generateRandomExpression()
    {
        Expression expression = new Expression(indexContext);

        int equality = getRandom().nextIntBetween(0, 100);
        int lower = getRandom().nextIntBetween(0, 75);
        int upper = getRandom().nextIntBetween(25, 100);
        while (upper <= lower)
            upper = getRandom().nextIntBetween(0, 100);

        if (getRandom().nextBoolean())
            expression.add(Operator.EQ, Int32Type.instance.decompose(equality));
        else
        {
            boolean useLower = getRandom().nextBoolean();
            boolean useUpper = getRandom().nextBoolean();
            if (!useLower && !useUpper)
                useLower = useUpper = true;
            if (useLower)
                expression.add(getRandom().nextBoolean() ? Operator.GT : Operator.GTE, Int32Type.instance.decompose(lower));
            if (useUpper)
                expression.add(getRandom().nextBoolean() ? Operator.LT : Operator.LTE, Int32Type.instance.decompose(upper));
        }
        return expression;
    }

    private AbstractBounds<PartitionPosition> generateRandomBounds(List<DecoratedKey> keys)
    {
        PartitionPosition leftBound = getRandom().nextBoolean() ? partitioner.getMinimumToken().minKeyBound()
                                                                : keys.get(getRandom().nextIntBetween(0, keys.size() - 1)).getToken().minKeyBound();

        PartitionPosition rightBound = getRandom().nextBoolean() ? partitioner.getMinimumToken().minKeyBound()
                                                                 : keys.get(getRandom().nextIntBetween(0, keys.size() - 1)).getToken().maxKeyBound();

        AbstractBounds<PartitionPosition> keyRange;

        if (leftBound.isMinimum() && rightBound.isMinimum())
            keyRange = new Range<>(leftBound, rightBound);
        else
        {
            if (AbstractBounds.strictlyWrapsAround(leftBound, rightBound))
            {
                PartitionPosition temp = leftBound;
                leftBound = rightBound;
                rightBound = temp;
            }
            if (getRandom().nextBoolean())
                keyRange = new Bounds<>(leftBound, rightBound);
            else if (getRandom().nextBoolean())
                keyRange = new ExcludingBounds<>(leftBound, rightBound);
            else
                keyRange = new IncludingExcludingBounds<>(leftBound, rightBound);
        }
        return keyRange;
    }

    private int termFromComparable(ByteComparable comparable)
    {
        ByteSource.Peekable peekable = ByteSource.peekable(comparable.asComparableBytes(ByteComparable.Version.OSS50));
        return Int32Type.instance.compose(Int32Type.instance.fromComparableBytes(peekable, ByteComparable.Version.OSS50));
    }

    private Map<Integer, Set<DecoratedKey>> buildTermMap()
    {
        Map<Integer, Set<DecoratedKey>> terms = new HashMap<>();

        for (int count = 0; count < 10000; count++)
        {
            int term = getRandom().nextIntBetween(0, 100);
            Set<DecoratedKey> pks;
            if (terms.containsKey(term))
                pks = terms.get(term);
            else
            {
                pks = new HashSet<>();
                terms.put(term, pks);
            }
            DecoratedKey key = makeKey(cfs.metadata(), getRandom().nextIntBetween(0, 20000));
            while (pks.contains(key))
                key = makeKey(cfs.metadata(), getRandom().nextIntBetween(0, 20000));
            pks.add(key);
        }
        return terms;
    }

    private void addRow(int pk, int value)
    {
        DecoratedKey key = makeKey(cfs.metadata(), pk);
        memtableIndex.index(key,
                            Clustering.EMPTY,
                            Int32Type.instance.decompose(value),
                            cfs.getCurrentMemtable(),
                            new OpOrder().start());
        keyMap.put(key, pk);
    }

    private DecoratedKey makeKey(TableMetadata table, Integer partitionKey)
    {
        ByteBuffer key = table.partitionKeyType.fromString(partitionKey.toString());
        return table.partitioner.decorateKey(key);
    }

    private void setTrieMemtableBufferType(final BufferType newBufferType) throws Exception
    {
        Field bufferType = TrieMemtable.class.getDeclaredField("BUFFER_TYPE");
        bufferType.setAccessible(true);
        Field modifiersField = Field.class.getDeclaredField("modifiers");
        modifiersField.setAccessible(true);
        modifiersField.setInt(bufferType, bufferType.getModifiers() & ~Modifier.FINAL);
        bufferType.set(null, newBufferType);
    }
}

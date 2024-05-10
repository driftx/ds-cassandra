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

package org.apache.cassandra.index.sai.disk.v1;

import java.io.IOException;
import java.nio.ByteBuffer;
import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.index.sai.disk.PrimaryKeyMap;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.v1.bitpack.BlockPackedReader;
import org.apache.cassandra.index.sai.disk.v1.bitpack.MonotonicBlockPackedReader;
import org.apache.cassandra.index.sai.disk.v1.bitpack.NumericValuesMeta;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.io.sstable.IKeyFetcher;
import org.apache.cassandra.io.sstable.SSTableId;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.Throwables;

/**
 * A partition-aware {@link PrimaryKeyMap}
 *
 * This uses the following on-disk structures:
 * <ul>
 *     <li>Block-packed structure for rowId to token lookups using {@link BlockPackedReader}.
 *     Uses component {@link IndexComponent#TOKEN_VALUES} </li>
 *     <li>Monotonic-block-packed structure for rowId to partition key offset lookups using {@link MonotonicBlockPackedReader}.
 *     Uses component {@link IndexComponent#OFFSETS_VALUES} </li>
 * </ul>
 *
 * This uses a {@link IKeyFetcher} to read the {@link org.apache.cassandra.db.DecoratedKey} for a {@link PrimaryKey} from the
 * sstable using the sstable offset provided by the monotonic-block-packed structure above.
 */
@NotThreadSafe
public class PartitionAwarePrimaryKeyMap implements PrimaryKeyMap
{
    @ThreadSafe
    public static class PartitionAwarePrimaryKeyMapFactory implements Factory
    {
        private final LongArray.Factory tokenReaderFactory;
        private final LongArray.Factory offsetReaderFactory;
        private final MetadataSource metadata;
        private final SSTableReader sstable;
        private final IPartitioner partitioner;
        private final PrimaryKey.Factory primaryKeyFactory;
        private final SSTableId<?> sstableId;

        private FileHandle token = null;
        private FileHandle offset = null;

        public PartitionAwarePrimaryKeyMapFactory(IndexDescriptor indexDescriptor, SSTableReader sstable)
        {
            try
            {
                this.metadata = MetadataSource.loadGroupMetadata(indexDescriptor);
                NumericValuesMeta offsetsMeta = new NumericValuesMeta(this.metadata.get(indexDescriptor.componentFileName(IndexComponent.OFFSETS_VALUES)));
                NumericValuesMeta tokensMeta = new NumericValuesMeta(this.metadata.get(indexDescriptor.componentFileName(IndexComponent.TOKEN_VALUES)));

                token = indexDescriptor.createPerSSTableFileHandle(IndexComponent.TOKEN_VALUES);
                offset = indexDescriptor.createPerSSTableFileHandle(IndexComponent.OFFSETS_VALUES);

                this.tokenReaderFactory = new BlockPackedReader(token, tokensMeta);
                this.offsetReaderFactory = new MonotonicBlockPackedReader(offset, offsetsMeta);
                this.partitioner = indexDescriptor.partitioner;
                this.sstable = sstable;
                this.primaryKeyFactory = indexDescriptor.primaryKeyFactory;
                this.sstableId = sstable.getId();
            }
            catch (Throwable t)
            {
                throw Throwables.unchecked(Throwables.close(t, token, offset));
            }
        }

        @Override
        public PrimaryKeyMap newPerSSTablePrimaryKeyMap()
        {
            LongArray rowIdToToken = null;
            LongArray rowIdToOffset = null;
            IKeyFetcher keyFetcher = null;
            try
            {
                rowIdToToken = new LongArray.DeferredLongArray(() -> tokenReaderFactory.open());
                rowIdToOffset = new LongArray.DeferredLongArray(() -> offsetReaderFactory.open());
                keyFetcher = sstable.openKeyFetcher(false);

                return new PartitionAwarePrimaryKeyMap(rowIdToToken, rowIdToOffset, partitioner, keyFetcher, primaryKeyFactory, sstableId);
            }
            catch (RuntimeException | Error e)
            {
                Throwables.closeNonNullAndAddSuppressed(e, rowIdToToken, rowIdToOffset, keyFetcher);
            }
            return null;
        }

        @Override
        public void close() throws IOException
        {
            FileUtils.closeQuietly(offset, token);
        }
    }

    private final LongArray rowIdToToken;
    private final LongArray rowIdToOffset;
    private final IPartitioner partitioner;
    private final IKeyFetcher keyFetcher;
    private final PrimaryKey.Factory primaryKeyFactory;
    private final SSTableId<?> sstableId;
    private final ByteBuffer tokenBuffer = ByteBuffer.allocate(Long.BYTES);

    private PartitionAwarePrimaryKeyMap(LongArray rowIdToToken,
                                        LongArray rowIdToOffset,
                                        IPartitioner partitioner,
                                        IKeyFetcher keyFetcher,
                                        PrimaryKey.Factory primaryKeyFactory,
                                        SSTableId<?> sstableId)
    {
        this.rowIdToToken = rowIdToToken;
        this.rowIdToOffset = rowIdToOffset;
        this.partitioner = partitioner;
        this.keyFetcher = keyFetcher;
        this.primaryKeyFactory = primaryKeyFactory;
        this.sstableId = sstableId;
    }

    @Override
    public SSTableId<?> getSSTableId()
    {
        return sstableId;
    }

    @Override
    public PrimaryKey primaryKeyFromRowId(long sstableRowId)
    {
        tokenBuffer.putLong(rowIdToToken.get(sstableRowId));
        tokenBuffer.rewind();
        return primaryKeyFactory.createDeferred(partitioner.getTokenFactory().fromByteArray(tokenBuffer), () -> supplier(sstableRowId));
    }

    @Override
    public long exactRowIdOrInvertedCeiling(PrimaryKey key)
    {
        return rowIdToToken.indexOf(key.token().getLongValue());
    }

    @Override
    public long ceiling(PrimaryKey key)
    {
        var rowId = exactRowIdOrInvertedCeiling(key);
        if (rowId >= 0)
            return rowId;
        if (rowId == Long.MIN_VALUE)
            return -1;
        else
            return -rowId - 1;
    }

    @Override
    public long floor(PrimaryKey key)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long count()
    {
        return rowIdToToken.length();
    }

    @Override
    public void close() throws IOException
    {
        FileUtils.closeQuietly(rowIdToToken, rowIdToOffset, keyFetcher);
    }

    private PrimaryKey supplier(long sstableRowId)
    {
        return primaryKeyFactory.createPartitionKeyOnly(keyFetcher.apply(rowIdToOffset.get(sstableRowId)));
    }
}

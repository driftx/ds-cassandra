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
package org.apache.cassandra.db.commitlog;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import net.openhft.chronicle.core.util.ThrowingFunction;
import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.SimpleCachedBufferPool;
import org.apache.cassandra.utils.NativeLibrary;
import org.apache.cassandra.utils.INativeLibrary;
import org.apache.cassandra.utils.SyncUtil;

import static org.apache.cassandra.config.CassandraRelevantProperties.COMMITLOG_SKIP_FILE_ADVICE;

/*
 * Memory-mapped segment. Maps the destination channel into an appropriately-sized memory-mapped buffer in which the
 * mutation threads write. On sync forces the buffer to disk.
 * If possible, recycles used segment files to avoid reallocating large chunks of disk.
 */
public class MemoryMappedSegment extends CommitLogSegment
{
    @VisibleForTesting
    final int fd;

    @VisibleForTesting
    static boolean skipFileAdviseToFreePageCache = COMMITLOG_SKIP_FILE_ADVICE.getBoolean();

    /**
     * Constructs a new segment file.
     */
    MemoryMappedSegment(AbstractCommitLogSegmentManager manager, ThrowingFunction<Path, FileChannel, IOException> channelFactory)
    {
        super(manager, channelFactory);
        // mark the initial sync marker as uninitialised
        int firstSync = buffer.position();
        buffer.putInt(firstSync + 0, 0);
        buffer.putInt(firstSync + 4, 0);
        fd = NativeLibrary.instance.getfd(channel);
    }

    @Override
    protected ByteBuffer createBuffer()
    {
        try
        {
            MappedByteBuffer mappedFile = channel.map(FileChannel.MapMode.READ_WRITE, 0, DatabaseDescriptor.getCommitLogSegmentSize());
            manager.addSize(DatabaseDescriptor.getCommitLogSegmentSize());
            return mappedFile;
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, logFile);
        }
    }

    @Override
    void write(int startMarker, int nextMarker)
    {
        // if there's room in the discard section to write an empty header,
        // zero out the next sync marker so replayer can cleanly exit
        if (nextMarker <= buffer.capacity() - SYNC_MARKER_SIZE)
        {
            buffer.putInt(nextMarker, 0);
            buffer.putInt(nextMarker + 4, 0);
        }

        // write previous sync marker to point to next sync marker
        // we don't chain the crcs here to ensure this method is idempotent if it fails
        writeSyncMarker(id, buffer, startMarker, startMarker, nextMarker);
    }

    @Override
    protected void flush(int startMarker, int nextMarker)
    {
        try
        {
            SyncUtil.force((MappedByteBuffer) buffer);
        }
        catch (Exception e) // MappedByteBuffer.force() does not declare IOException but can actually throw it
        {
            throw new FSWriteError(e, getPath());
        }

        if (!skipFileAdviseToFreePageCache)
        {
            adviceOnFileToFreePageCache(fd, startMarker, nextMarker, logFile);
        }
    }

    void adviceOnFileToFreePageCache(int fd, int startMarker, int nextMarker, File logFile)
    {
        INativeLibrary.instance.trySkipCache(fd, startMarker, nextMarker, logFile.absolutePath());
    }

    @Override
    public long onDiskSize()
    {
        return DatabaseDescriptor.getCommitLogSegmentSize();
    }

    @Override
    protected void internalClose()
    {
        FileUtils.clean(buffer);
        super.internalClose();
    }

    protected static class MemoryMappedSegmentBuilder extends CommitLogSegment.Builder
    {
        public MemoryMappedSegmentBuilder(AbstractCommitLogSegmentManager segmentManager)
        {
            super(segmentManager);
        }

        @Override
        public MemoryMappedSegment build()
        {
            return new MemoryMappedSegment(segmentManager,
                                           path ->  FileChannel.open(path, StandardOpenOption.WRITE, StandardOpenOption.READ, StandardOpenOption.CREATE));
        }

        @Override
        public SimpleCachedBufferPool createBufferPool()
        {
            return null;
        }
    }
}

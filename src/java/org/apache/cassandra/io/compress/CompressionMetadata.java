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
package org.apache.cassandra.io.compress;

import java.io.DataOutput;
import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Longs;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileInputStreamPlus;
import org.apache.cassandra.io.util.FileOutputStreamPlus;
import org.apache.cassandra.io.util.Memory;
import org.apache.cassandra.io.util.SafeMemory;
import org.apache.cassandra.io.util.SliceDescriptor;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.concurrent.Ref;
import org.apache.cassandra.utils.concurrent.Transactional;
import org.apache.cassandra.utils.concurrent.WrappedSharedCloseable;

/**
 * Holds metadata about compressed file
 * TODO extract interface ICompressionMetadata which will just provide non-resource properties
 */
public class CompressionMetadata extends WrappedSharedCloseable
{
    /**
     * DataLength can represent either the true length of the file
     * or some shorter value, in the case we want to impose a shorter limit on readers
     * (when early opening, we want to ensure readers cannot read past fully written sections).
     * If zero copy metadata is present, this is the uncompressed length of the partial data file.
     */
    public final long dataLength;

    /**
     * Length of the compressed file in bytes. This refers to the partial file length if zero copy metadata is present.
     */
    public final long compressedFileLength;

    /**
     * Offsets of consecutive chunks in the (compressed) data file. The length of this array is equal to the number of
     * chunks. Each item is of Long type, thus 8 bytes long. Note that even if we deal with a partial data file (zero
     * copy metadata is present), we store offsets of all chunks for the original (compressed) data file.
     */
    private final Memory.LongArray chunkOffsets;
    public final File chunksIndexFile;
    public final CompressionParams parameters;

    /**
     * The length of the chunk in bits. The chunk length must be a power of 2, so this is the number of trailing zeros
     * in the chunk length.
     */
    private final int chunkLengthBits;

    /**
     * If we don't want to load the all offsets into memory, for example when we deal with a slice, this is the index of
     * the first offset we loaded.
     */
    private final int startChunkIndex;

    public static CompressionMetadata open(File chunksIndexFile, long compressedLength, boolean hasMaxCompressedSize)
    {
        return open(chunksIndexFile, compressedLength, hasMaxCompressedSize, SliceDescriptor.NONE);
    }

    @VisibleForTesting
    public static CompressionMetadata open(File chunksIndexFile, long compressedLength, boolean hasMaxCompressedSize, SliceDescriptor sliceDescriptor)
    {
        long uncompressedOffset = sliceDescriptor.exists() ? sliceDescriptor.sliceStart : 0;
        long uncompressedLength = sliceDescriptor.exists() ? sliceDescriptor.dataEnd - sliceDescriptor.sliceStart : -1;

        CompressionParams parameters;
        long dataLength;
        Memory.LongArray chunkOffsets;

        try (FileInputStreamPlus stream = chunksIndexFile.newInputStream())
        {
            String compressorName = stream.readUTF();
            int optionCount = stream.readInt();
            Map<String, String> options = new HashMap<>(optionCount);
            for (int i = 0; i < optionCount; ++i)
            {
                String key = stream.readUTF();
                String value = stream.readUTF();
                options.put(key, value);
            }
            int chunkLength = stream.readInt();
            int maxCompressedSize = Integer.MAX_VALUE;
            if (hasMaxCompressedSize)
                maxCompressedSize = stream.readInt();
            try
            {
                parameters = new CompressionParams(compressorName, chunkLength, maxCompressedSize, options);
            }
            catch (ConfigurationException e)
            {
                throw new RuntimeException("Cannot create CompressionParams for stored parameters", e);
            }

            assert Integer.bitCount(chunkLength) == 1;
            int chunkLengthBits = Integer.numberOfTrailingZeros(chunkLength);
            long readDataLength = stream.readLong();
            dataLength = uncompressedLength >= 0 ? uncompressedLength : readDataLength;

            int startChunkIndex = Math.toIntExact(uncompressedOffset >> chunkLengthBits);
            assert uncompressedOffset == (long) startChunkIndex << chunkLengthBits;

            int endChunkIndex = Math.toIntExact((uncompressedOffset + dataLength - 1) >> chunkLengthBits) + 1;

            Pair<Memory.LongArray, Long> offsetsAndLimit = readChunkOffsets(stream, startChunkIndex, endChunkIndex, compressedLength);
            chunkOffsets = offsetsAndLimit.left;
            // We adjust the compressed file length to store the position after the last chunk just to be able to
            // calculate the offset of the chunk next to the last one (in order to calculate the length of the last chunk).
            // Obvously, we could use the compressed file length for that purpose but unfortunately, sometimes there is
            // an empty chunk added to the end of the file thus we cannot rely on the file length.
            long compressedFileLength = offsetsAndLimit.right;

            return new CompressionMetadata(chunksIndexFile, parameters, chunkOffsets, dataLength, compressedFileLength, chunkLengthBits, startChunkIndex);
        }
        catch (FileNotFoundException | NoSuchFileException e)
        {
            throw new RuntimeException(e);
        }
        catch (IOException e)
        {
            throw new CorruptSSTableException(e, chunksIndexFile);
        }
    }

    // do not call this constructor directly, unless used in testing
    @VisibleForTesting
    public CompressionMetadata(File chunksIndexFile,
                               CompressionParams parameters,
                               Memory.LongArray chunkOffsets,
                               long dataLength,
                               long compressedFileLength,
                               int chunkLengthBits,
                               int startChunkIndex)
    {
        super(chunkOffsets);
        this.chunksIndexFile = chunksIndexFile;
        this.parameters = parameters;
        this.dataLength = dataLength;
        this.compressedFileLength = compressedFileLength;
        this.chunkOffsets = chunkOffsets;
        this.chunkLengthBits = chunkLengthBits;
        this.startChunkIndex = startChunkIndex;
    }

    private CompressionMetadata(CompressionMetadata copy)
    {
        super(copy);
        this.chunksIndexFile = copy.chunksIndexFile;
        this.parameters = copy.parameters;
        this.dataLength = copy.dataLength;
        this.compressedFileLength = copy.compressedFileLength;
        this.chunkOffsets = copy.chunkOffsets;
        this.chunkLengthBits = copy.chunkLengthBits;
        this.startChunkIndex = copy.startChunkIndex;
    }

    public ICompressor compressor()
    {
        return parameters.getSstableCompressor();
    }

    public int chunkLength()
    {
        return parameters.chunkLength();
    }

    public int maxCompressedLength()
    {
        return parameters.maxCompressedLength();
    }

    /**
     * Returns the amount of memory in bytes used off heap.
     * @return the amount of memory in bytes used off heap
     */
    public long offHeapSize()
    {
        return chunkOffsets.memory.size();
    }

    @Override
    public void addTo(Ref.IdentityCollection identities)
    {
        super.addTo(identities);
        identities.add(chunkOffsets.memory);
    }

    @Override
    public CompressionMetadata sharedCopy()
    {
        return new CompressionMetadata(this);
    }

    /**
     * Reads offsets of the individual chunks from the given input, filtering out non-relevant offsets (outside the
     * specified range).
     *
     * @param input Source of the data
     * @param startIndex Index of the first chunk to read, inclusive
     * @param endIndex Index of the last chunk to read, exclusive
     * @param compressedFileLength compressed file length
     *
     * @return A pair of chunk offsets array and the offset next to the last read chunk
     */
    private static Pair<Memory.LongArray, Long> readChunkOffsets(FileInputStreamPlus input, int startIndex, int endIndex, long compressedFileLength)
    {
        final Memory.LongArray offsets;
        final int chunkCount;
        try
        {
            chunkCount = input.readInt();
            if (chunkCount <= 0)
                throw new IOException("Compressed file with 0 chunks encountered: " + input);
        }
        catch (IOException e)
        {
            throw new FSReadError(e, input.file);
        }

        Preconditions.checkState(startIndex < chunkCount, "The start index %s has to be < chunk count %s", startIndex, chunkCount);
        Preconditions.checkState(endIndex <= chunkCount, "The end index %s has to be <= chunk count %s", endIndex, chunkCount);
        Preconditions.checkState(startIndex <= endIndex, "The start index %s has to be < end index %s", startIndex, endIndex);

        int chunksToRead = endIndex - startIndex;

        if (chunksToRead == 0)
            return Pair.create(new Memory.LongArray(0), 0L);

        offsets = new Memory.LongArray(chunksToRead);
        long i = 0;
        try
        {
            input.skipBytes(startIndex * 8);
            long lastOffset;
            for (i = 0; i < chunksToRead; i++)
            {
                lastOffset = input.readLong();
                offsets.set(i, lastOffset);
            }

            lastOffset = endIndex < chunkCount ? input.readLong() - offsets.get(0) : compressedFileLength;
            return Pair.create(offsets, lastOffset);
        }
        catch (EOFException e)
        {
            offsets.close();
            String msg = String.format("Corrupted Index File %s: read %d but expected at least %d chunks.",
                                       input, i, chunksToRead);
            throw new CorruptSSTableException(new IOException(msg, e), input.file);
        }
        catch (IOException e)
        {
            offsets.close();
            throw new FSReadError(e, input.file);
        }
    }

    /**
     * Get a chunk of compressed data (offset, length) corresponding to given position
     *
     * @param uncompressedDataPosition Position in the uncompressed data. If we deal with a slice, this is the position
     *                                 in the original uncompressed data.
     * @return A pair of chunk offset and length. If we deal with a slice, the chunk offset refers to the position in
     * the compressed slice.
     */
    public Chunk chunkFor(long uncompressedDataPosition)
    {
        int chunkIdx = chunkIndex(uncompressedDataPosition);
        return chunk(chunkIdx);
    }

    private Chunk chunk(long chunkOffset, long nextChunkOffset)
    {
        return new Chunk(chunkOffset, Math.toIntExact(nextChunkOffset - chunkOffset - 4)); // "4" bytes reserved for checksum
    }

    private Chunk chunk(int chunkIdx)
    {
        long chunkOffset = chunkOffset(chunkIdx);
        long nextChunkOffset = nextChunkOffset(chunkIdx);
        return chunk(chunkOffset, nextChunkOffset);
    }

    private long nextChunkOffset(int chunkIdx)
    {
        if (chunkIdx == chunkOffsets.size() - 1)
            return compressedFileLength + chunkOffsets.get(0);
        return chunkOffset(chunkIdx + 1);
    }

    private long chunkOffset(int chunkIdx)
    {
        if (chunkIdx >= chunkOffsets.size())
            throw new CorruptSSTableException(new EOFException(String.format("Chunk %d out of bounds: %d", chunkIdx, chunkOffsets.size())), chunksIndexFile);

        return chunkOffsets.get(chunkIdx);
    }

    private int chunkIndex(long uncompressedDataPosition)
    {
        return Math.toIntExact(uncompressedDataPosition >> chunkLengthBits) - startChunkIndex;
    }

    /**
     * Searches for the chunk with the given offset and returns the offset of uncompressed data for the found chunk.
     * @param chunkOffset exact chunk offset to search for; if we deal with a slice this is a chunk offset
     *                    in the original compressed file
     * @return offset of uncompressed data for the found chunk; if we deal with a slice this is the offset
     * in the original uncompressed data
     * @throws IllegalArgumentException if no chunk with the given offset is found
     */
    public long getDataOffsetForChunkOffset(long chunkOffset)
    {
        long l = 0;
        long h = chunkOffsets.size() - 1;
        long idx, offset;

        while (l <= h)
        {
            idx = (l + h) >>> 1;
            offset = chunkOffsets.get(idx);

            if (offset < chunkOffset)
                l = idx + 1;
            else if (offset > chunkOffset)
                h = idx - 1;
            else
                return (idx + startChunkIndex) << chunkLengthBits;
        }

        throw new IllegalArgumentException("No chunk with offset " + chunkOffset);
    }

    /**
     * @param sections Collection of sections in uncompressed file. Should not contain sections that overlap each other.
     * @return Total chunk size in bytes for given sections including checksum.
     */
    public long getTotalSizeForSections(Collection<SSTableReader.PartitionPositionBounds> sections)
    {
        long size = 0;
        int lastIncludedChunkIdx = -1;
        for (SSTableReader.PartitionPositionBounds section : sections)
        {
            int sectionStartIdx = Math.max(chunkIndex(section.lowerPosition), lastIncludedChunkIdx + 1);
            int sectionEndIdx = chunkIndex(section.upperPosition - 1); // we need to include the last byte of the seciont but not the upper position (which is excludded)

            for (int idx = sectionStartIdx; idx <= sectionEndIdx; idx++)
            {
                long chunkOffset = chunkOffset(idx);
                long nextChunkOffset = nextChunkOffset(idx);
                size += nextChunkOffset - chunkOffset;
            }
            lastIncludedChunkIdx = sectionEndIdx;
        }
        return size;
    }

    /**
     * @param sections Collection of sections in uncompressed data. If we deal with a slice, the sections refer to the
     *                 positions in the original uncompressed data.
     * @return Array of chunks which corresponds to given sections of uncompressed file, sorted by chunk offset.
     * Note that if we deal with a slice, the chunk offsets refer to the positions in the compressed slice.
     */
    public Chunk[] getChunksForSections(Collection<SSTableReader.PartitionPositionBounds> sections)
    {
        // use SortedSet to eliminate duplicates and sort by chunk offset
        SortedSet<Chunk> offsets = new TreeSet<>((o1, o2) -> Longs.compare(o1.offset, o2.offset));

        for (SSTableReader.PartitionPositionBounds section : sections)
        {
            int sectionStartIdx = chunkIndex(section.lowerPosition);
            int sectionEndIdx = chunkIndex(section.upperPosition - 1); // we need to include the last byte of the seciont but not the upper position (which is excludded)

            for (int idx = sectionStartIdx; idx <= sectionEndIdx; idx++)
                offsets.add(chunk(idx));
        }

        return offsets.toArray(new Chunk[offsets.size()]);
    }

    public static class Writer extends Transactional.AbstractTransactional implements Transactional
    {
        // path to the file
        private final CompressionParams parameters;
        private final File file;
        private int maxCount = 100;
        private SafeMemory offsets = new SafeMemory(maxCount * 8L);
        private int count = 0;

        // provided by user when setDescriptor
        private long dataLength, chunkCount;

        private Writer(CompressionParams parameters, File file)
        {
            this.parameters = parameters;
            this.file = file;
        }

        public static Writer open(CompressionParams parameters, File file)
        {
            return new Writer(parameters, file);
        }

        public void addOffset(long offset)
        {
            if (count == maxCount)
            {
                SafeMemory newOffsets = offsets.copy((maxCount *= 2L) * 8L);
                offsets.close();
                offsets = newOffsets;
            }
            offsets.setLong(8L * count++, offset);
        }

        private void writeHeader(DataOutput out, long dataLength, int chunks)
        {
            try
            {
                out.writeUTF(parameters.getSstableCompressor().getClass().getSimpleName());
                out.writeInt(parameters.getOtherOptions().size());
                for (Map.Entry<String, String> entry : parameters.getOtherOptions().entrySet())
                {
                    out.writeUTF(entry.getKey());
                    out.writeUTF(entry.getValue());
                }

                // store the length of the chunk
                out.writeInt(parameters.chunkLength());
                out.writeInt(parameters.maxCompressedLength());
                // store position and reserve a place for uncompressed data length and chunks count
                out.writeLong(dataLength);
                out.writeInt(chunks);
            }
            catch (IOException e)
            {
                throw new FSWriteError(e, file);
            }
        }

        // we've written everything; wire up some final metadata state
        public Writer finalizeLength(long dataLength, int chunkCount)
        {
            this.dataLength = dataLength;
            this.chunkCount = chunkCount;
            return this;
        }

        @Override
        public void doPrepare()
        {
            assert chunkCount == count;

            // finalize the size of memory used if it won't now change;
            // unnecessary if already correct size
            if (offsets.size() != count * 8L)
            {
                SafeMemory tmp = offsets;
                offsets = offsets.copy(count * 8L);
                tmp.free();
            }

            // flush the data to disk
            try (FileOutputStreamPlus out = file.newOutputStream(File.WriteMode.OVERWRITE))
            {
                writeHeader(out, dataLength, count);
                for (int i = 0; i < count; i++)
                    out.writeLong(offsets.getLong(i * 8L));

                out.flush();
                out.sync();
            }
            catch (FileNotFoundException | NoSuchFileException fnfe)
            {
                throw new RuntimeException(fnfe);
            }
            catch (IOException e)
            {
                throw new FSWriteError(e, file);
            }
        }

        public CompressionMetadata open(long dataLength, long compressedLength)
        {
            SafeMemory tOffsets = this.offsets.sharedCopy();

            // calculate how many entries we need, if our dataLength is truncated
            int tCount = (int) (dataLength / parameters.chunkLength());
            if (dataLength % parameters.chunkLength() != 0)
                tCount++;

            assert tCount > 0;
            // grab our actual compressed length from the next offset from our the position we're opened to
            if (tCount < this.count)
                compressedLength = tOffsets.getLong(tCount * 8L);

            return new CompressionMetadata(file, parameters, new Memory.LongArray(tOffsets, tCount), dataLength, compressedLength, Integer.numberOfTrailingZeros(parameters.chunkLength()), 0);
        }

        /**
         * Get a chunk offset by its index.
         *
         * @param chunkIndex Index of the chunk.
         *
         * @return offset of the chunk in the compressed file.
         */
        public long chunkOffsetBy(int chunkIndex)
        {
            return offsets.getLong(chunkIndex * 8L);
        }

        /**
         * Reset the writer so that the next chunk offset written will be the
         * one of {@code chunkIndex}.
         *
         * @param chunkIndex the next index to write
         */
        public void resetAndTruncate(int chunkIndex)
        {
            count = chunkIndex;
        }

        @Override
        protected Throwable doPostCleanup(Throwable failed)
        {
            return offsets.close(failed);
        }

        @Override
        protected Throwable doCommit(Throwable accumulate)
        {
            return accumulate;
        }

        @Override
        protected Throwable doAbort(Throwable accumulate)
        {
            return accumulate;
        }
    }

    /**
     * Holds offset and length of the file chunk
     */
    public static class Chunk
    {
        public static final IVersionedSerializer<Chunk> serializer = new ChunkSerializer();

        public final long offset;
        public final int length;

        public Chunk(long offset, int length)
        {
            assert(length >= 0);

            this.offset = offset;
            this.length = length;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Chunk chunk = (Chunk) o;
            return length == chunk.length && offset == chunk.offset;
        }

        @Override
        public int hashCode()
        {
            int result = (int) (offset ^ (offset >>> 32));
            result = 31 * result + length;
            return result;
        }

        @Override
        public String toString()
        {
            return String.format("Chunk<offset: %d, length: %d>", offset, length);
        }
    }

    static class ChunkSerializer implements IVersionedSerializer<Chunk>
    {
        @Override
        public void serialize(Chunk chunk, DataOutputPlus out, int version) throws IOException
        {
            out.writeLong(chunk.offset);
            out.writeInt(chunk.length);
        }

        @Override
        public Chunk deserialize(DataInputPlus in, int version) throws IOException
        {
            return new Chunk(in.readLong(), in.readInt());
        }

        @Override
        public long serializedSize(Chunk chunk, int version)
        {
            long size = TypeSizes.sizeof(chunk.offset);
            size += TypeSizes.sizeof(chunk.length);
            return size;
        }
    }
}

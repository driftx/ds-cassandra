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
package org.apache.cassandra.db.marshal;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import org.apache.cassandra.cql3.Maps;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.serializers.CollectionSerializer;
import org.apache.cassandra.serializers.MapSerializer;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.JsonUtils;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteComparable.Version;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;

public class MapType<K, V> extends CollectionType<Map<K, V>>
{
    // interning instances
    private static final ConcurrentHashMap<Pair<AbstractType<?>, AbstractType<?>>, MapType> instances = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<Pair<AbstractType<?>, AbstractType<?>>, MapType> frozenInstances = new ConcurrentHashMap<>();

    private final AbstractType<K> keys;
    private final AbstractType<V> values;
    private final MapSerializer<K, V> serializer;

    public static MapType<?, ?> getInstance(TypeParser parser) throws ConfigurationException, SyntaxException
    {
        List<AbstractType<?>> l = parser.getTypeParameters();
        if (l.size() != 2)
            throw new ConfigurationException("MapType takes exactly 2 type parameters");

        return getInstance(l.get(0).freeze(), l.get(1).freeze(), true);
    }

    @SuppressWarnings("unchecked")
    public static <K, V> MapType<K, V> getInstance(AbstractType<K> keys, AbstractType<V> values, boolean isMultiCell)
    {
        return getInstance(isMultiCell ? instances : frozenInstances, Pair.create(keys, values), () -> new MapType<>(keys, values, isMultiCell));
    }

    private MapType(AbstractType<K> keys, AbstractType<V> values, boolean isMultiCell)
    {
        super(ComparisonType.CUSTOM, Kind.MAP, isMultiCell, ImmutableList.of(keys, values));
        this.keys = keys;
        this.values = values;
        this.serializer = MapSerializer.getInstance(keys.getSerializer(),
                                                    values.getSerializer(),
                                                    keys.comparatorSet);
    }

    @Override
    @SuppressWarnings("unchecked")
    public MapType<K, V> with(ImmutableList<AbstractType<?>> subTypes, boolean isMultiCell)
    {
        Preconditions.checkArgument(subTypes.size() == 2, "Invalid number of subTypes for MapType (got %s)", subTypes.size());

        if (subTypes.equals(this.subTypes()) && isMultiCell == this.isMultiCell())
            return this;

        return getInstance((AbstractType<K>) subTypes.get(0), (AbstractType<V>) subTypes.get(1), isMultiCell);
    }

    public AbstractType<K> getKeysType()
    {
        return keys;
    }

    public AbstractType<V> getValuesType()
    {
        return values;
    }

    @Override
    public AbstractType<K> nameComparator()
    {
        return keys;
    }

    public AbstractType<V> valueComparator()
    {
        return values;
    }

    public <RL, TR> int compareCustom(RL left, ValueAccessor<RL> accessorL, TR right, ValueAccessor<TR> accessorR)
    {
        return compareMaps(keys, values, left, accessorL, right, accessorR);
    }

    public static <TL, TR> int compareMaps(AbstractType<?> keysComparator, AbstractType<?> valuesComparator, TL left, ValueAccessor<TL> accessorL, TR right, ValueAccessor<TR> accessorR)
    {
        if (accessorL.isEmpty(left) || accessorR.isEmpty(right))
            return Boolean.compare(accessorR.isEmpty(right), accessorL.isEmpty(left));


        int sizeL = CollectionSerializer.readCollectionSize(left, accessorL);
        int sizeR = CollectionSerializer.readCollectionSize(right, accessorR);

        int offsetL = CollectionSerializer.sizeOfCollectionSize();
        int offsetR = CollectionSerializer.sizeOfCollectionSize();

        for (int i = 0; i < Math.min(sizeL, sizeR); i++)
        {
            TL k1 = CollectionSerializer.readValue(left, accessorL, offsetL);
            offsetL += CollectionSerializer.sizeOfValue(k1, accessorL);
            TR k2 = CollectionSerializer.readValue(right, accessorR, offsetR);
            offsetR += CollectionSerializer.sizeOfValue(k2, accessorR);
            int cmp = keysComparator.compare(k1, accessorL, k2, accessorR);
            if (cmp != 0)
                return cmp;

            TL v1 = CollectionSerializer.readValue(left, accessorL, offsetL);
            offsetL += CollectionSerializer.sizeOfValue(v1, accessorL);
            TR v2 = CollectionSerializer.readValue(right, accessorR, offsetR);
            offsetR += CollectionSerializer.sizeOfValue(v2, accessorR);
            cmp = valuesComparator.compare(v1, accessorL, v2, accessorR);
            if (cmp != 0)
                return cmp;
        }

        return Integer.compare(sizeL, sizeR);
    }

    @Override
    public <T> ByteSource asComparableBytes(ValueAccessor<T> accessor, T data, Version version)
    {
        return asComparableBytesMap(getKeysType(), getValuesType(), accessor, data, version);
    }

    @Override
    public <T> T fromComparableBytes(ValueAccessor<T> accessor, ByteSource.Peekable comparableBytes, Version version)
    {
        return fromComparableBytesMap(accessor, comparableBytes, version, getKeysType(), getValuesType());
    }

    static <V> ByteSource asComparableBytesMap(AbstractType<?> keysComparator,
                                               AbstractType<?> valuesComparator,
                                               ValueAccessor<V> accessor,
                                               V data,
                                               Version version)
    {
        if (accessor.isEmpty(data))
            return null;

        int offset = 0;
        int size = CollectionSerializer.readCollectionSize(data, accessor);
        offset += CollectionSerializer.sizeOfCollectionSize();
        ByteSource[] srcs = new ByteSource[size * 2];
        for (int i = 0; i < size; ++i)
        {
            V k = CollectionSerializer.readValue(data, accessor, offset);
            offset += CollectionSerializer.sizeOfValue(k, accessor);
            srcs[i * 2 + 0] = keysComparator.asComparableBytes(accessor, k, version);
            V v = CollectionSerializer.readValue(data, accessor, offset);
            offset += CollectionSerializer.sizeOfValue(v, accessor);
            srcs[i * 2 + 1] = valuesComparator.asComparableBytes(accessor, v, version);
        }
        return ByteSource.withTerminatorMaybeLegacy(version, 0x00, srcs);
    }

    static <V> V fromComparableBytesMap(ValueAccessor<V> accessor,
                                        ByteSource.Peekable comparableBytes,
                                        Version version,
                                        AbstractType<?> keysComparator,
                                        AbstractType<?> valuesComparator)
    {
        if (comparableBytes == null)
            return accessor.empty();
        assert version != ByteComparable.Version.LEGACY; // legacy translation is not reversible

        List<V> buffers = new ArrayList<>();
        int separator = comparableBytes.next();
        while (separator != ByteSource.TERMINATOR)
        {
            buffers.add(ByteSourceInverse.nextComponentNull(separator)
                        ? null
                        : keysComparator.fromComparableBytes(accessor, comparableBytes, version));
            separator = comparableBytes.next();
            buffers.add(ByteSourceInverse.nextComponentNull(separator)
                        ? null
                        : valuesComparator.fromComparableBytes(accessor, comparableBytes, version));
            separator = comparableBytes.next();
        }
        return CollectionSerializer.pack(buffers, accessor,buffers.size() / 2);
    }

    @Override
    public MapSerializer<K, V> getSerializer()
    {
        return serializer;
    }

    @Override
    protected int collectionSize(List<ByteBuffer> values)
    {
        return values.size() / 2;
    }

    public List<ByteBuffer> serializedValues(Iterator<Cell<?>> cells)
    {
        assert isMultiCell;
        List<ByteBuffer> bbs = new ArrayList<ByteBuffer>();
        while (cells.hasNext())
        {
            Cell<?> c = cells.next();
            bbs.add(c.path().get(0));
            bbs.add(c.buffer());
        }
        return bbs;
    }

    @Override
    public Term fromJSONObject(Object parsed) throws MarshalException
    {
        if (parsed instanceof String)
            parsed = JsonUtils.decodeJson((String) parsed);

        if (!(parsed instanceof Map))
            throw new MarshalException(String.format(
                    "Expected a map, but got a %s: %s", parsed.getClass().getSimpleName(), parsed));

        Map<?, ?> map = (Map<?, ?>) parsed;
        Map<Term, Term> terms = new HashMap<>(map.size());
        for (Map.Entry<?, ?> entry : map.entrySet())
        {
            if (entry.getKey() == null)
                throw new MarshalException("Invalid null key in map");

            if (entry.getValue() == null)
                throw new MarshalException("Invalid null value in map");

            terms.put(keys.fromJSONObject(entry.getKey()), values.fromJSONObject(entry.getValue()));
        }
        return new Maps.DelayedValue(keys, terms);
    }

    @Override
    public String toJSONString(ByteBuffer buffer, ProtocolVersion protocolVersion)
    {
        ByteBuffer value = buffer.duplicate();
        StringBuilder sb = new StringBuilder("{");
        int size = CollectionSerializer.readCollectionSize(value, ByteBufferAccessor.instance);
        int offset = CollectionSerializer.sizeOfCollectionSize();
        for (int i = 0; i < size; i++)
        {
            if (i > 0)
                sb.append(", ");

            // map keys must be JSON strings, so convert non-string keys to strings
            ByteBuffer kv = CollectionSerializer.readValue(value, ByteBufferAccessor.instance, offset);
            offset += CollectionSerializer.sizeOfValue(kv, ByteBufferAccessor.instance);
            String key = keys.toJSONString(kv, protocolVersion);
            if (key.startsWith("\""))
                sb.append(key);
            else
                sb.append('"').append(JsonUtils.quoteAsJsonString(key)).append('"');

            sb.append(": ");
            ByteBuffer vv = CollectionSerializer.readValue(value, ByteBufferAccessor.instance, offset);
            offset += CollectionSerializer.sizeOfValue(vv, ByteBufferAccessor.instance);
            sb.append(values.toJSONString(vv, protocolVersion));
        }
        return sb.append("}").toString();
    }

    @Override
    public void forEach(ByteBuffer input, Consumer<ByteBuffer> action)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ByteBuffer getMaskedValue()
    {
        return decompose(Collections.emptyMap());
    }
}

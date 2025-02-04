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
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

import com.google.common.collect.ImmutableList;

import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.Constants;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.cql3.Tuples;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.serializers.CollectionSerializer;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.serializers.TupleSerializer;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.JsonUtils;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;

/**
 * This is essentially like a CompositeType, but it's not primarily meant for comparison, just
 * to pack multiple values together so has a more friendly encoding.
 */
public class TupleType extends MultiCellCapableType<ByteBuffer>
{
    private static final String COLON = ":";
    private static final Pattern COLON_PAT = Pattern.compile(COLON);
    private static final String ESCAPED_COLON = "\\\\:";
    private static final Pattern ESCAPED_COLON_PAT = Pattern.compile(ESCAPED_COLON);
    private static final String AT = "@";
    private static final Pattern AT_PAT = Pattern.compile(AT);
    private static final String ESCAPED_AT = "\\\\@";
    private static final Pattern ESCAPED_AT_PAT = Pattern.compile(ESCAPED_AT);
    private final TupleSerializer serializer;

    public TupleType(Iterable<AbstractType<?>> subTypes)
    {
        this(freeze(subTypes), false);
    }

    public TupleType(Iterable<AbstractType<?>> subTypes, boolean isMultiCell)
    {
        this(ImmutableList.copyOf(subTypes), isMultiCell);
    }

    public TupleType(ImmutableList<AbstractType<?>> subTypes, boolean isMultiCell)
    {
        super(ComparisonType.CUSTOM, isMultiCell, subTypes);
        this.serializer = new TupleSerializer(fieldSerializers(subTypes));
    }

    @Override
    public boolean allowsEmpty()
    {
        return true;
    }

    private static List<TypeSerializer<?>> fieldSerializers(List<AbstractType<?>> types)
    {
        int size = types.size();
        List<TypeSerializer<?>> serializers = new ArrayList<>(size);
        for (int i = 0; i < size; i++)
            serializers.add(types.get(i).getSerializer());
        return serializers;
    }

    public static TupleType getInstance(TypeParser parser) throws ConfigurationException, SyntaxException
    {
        List<AbstractType<?>> types = parser.getTypeParameters();
        return new TupleType(types, true);
    }

    @Override
    public TupleType with(ImmutableList<AbstractType<?>> subTypes, boolean isMultiCell)
    {
        return new TupleType(subTypes, isMultiCell);
    }

    @Override
    public ShortType nameComparator()
    {
        return ShortType.instance;
    }

    public AbstractType<?> type(int i)
    {
        return subTypes.get(i);
    }

    public int size()
    {
        return subTypes.size();
    }

    public boolean isTuple()
    {
        return true;
    }

    public <VL, VR> int compareCustom(VL left, ValueAccessor<VL> accessorL, VR right, ValueAccessor<VR> accessorR)
    {
        if (accessorL.isEmpty(left) || accessorR.isEmpty(right))
            return Boolean.compare(accessorR.isEmpty(right), accessorL.isEmpty(left));

        int offsetL = 0;
        int offsetR = 0;

        for (int i = 0; !accessorL.isEmptyFromOffset(left, offsetL) && !accessorR.isEmptyFromOffset(right, offsetR) && i < subTypes.size(); i++)
        {
            AbstractType<?> comparator = subTypes.get(i);

            int sizeL = accessorL.getInt(left, offsetL);
            offsetL += TypeSizes.INT_SIZE;
            int sizeR = accessorR.getInt(right, offsetR);
            offsetR += TypeSizes.INT_SIZE;

            // Handle nulls
            if (sizeL < 0)
            {
                if (sizeR < 0)
                    continue;
                return -1;
            }
            if (sizeR < 0)
                return 1;

            VL valueL = accessorL.slice(left, offsetL, sizeL);
            offsetL += sizeL;
            VR valueR = accessorR.slice(right, offsetR, sizeR);
            offsetR += sizeR;
            int cmp = comparator.compare(valueL, accessorL, valueR, accessorR);
            if (cmp != 0)
                return cmp;
        }

        if (allRemainingComponentsAreNull(left, accessorL, offsetL) && allRemainingComponentsAreNull(right, accessorR, offsetR))
            return 0;

        if (accessorL.isEmptyFromOffset(left, offsetL))
            return allRemainingComponentsAreNull(right, accessorR, offsetR) ? 0 : -1;

        return allRemainingComponentsAreNull(left, accessorL, offsetL) ? 0 : 1;
    }

    private <T> boolean allRemainingComponentsAreNull(T v, ValueAccessor<T> accessor, int offset)
    {
        while (!accessor.isEmptyFromOffset(v, offset))
        {
            int size = accessor.getInt(v, offset);
            offset += TypeSizes.INT_SIZE;
            if (size >= 0)
                return false;
        }
        return true;
    }

    @Override
    public <V> ByteSource asComparableBytes(ValueAccessor<V> accessor, V data, ByteComparable.Version version)
    {
        switch (version)
        {
            case LEGACY:
                return asComparableBytesLegacy(accessor, data);
            case OSS50:
                return asComparableBytesNew(accessor, data, version);
            default:
                throw new AssertionError();
        }
    }

    private <V> ByteSource asComparableBytesLegacy(ValueAccessor<V> accessor, V data)
    {
        if (accessor.isEmpty(data))
            return null;

        V[] bufs = split(accessor, data);  // this may be shorter than types.size -- other srcs remain null in that case
        ByteSource[] srcs = new ByteSource[subTypes.size()];
        for (int i = 0; i < bufs.length; ++i)
            srcs[i] = bufs[i] != null ? subTypes.get(i).asComparableBytes(accessor, bufs[i], ByteComparable.Version.LEGACY) : null;

        // We always have a fixed number of sources, with the trailing ones possibly being nulls.
        // This can only result in a prefix if the last type in the tuple allows prefixes. Since that type is required
        // to be weakly prefix-free, so is the tuple.
        return ByteSource.withTerminatorLegacy(ByteSource.END_OF_STREAM, srcs);
    }

    private <V> ByteSource asComparableBytesNew(ValueAccessor<V> accessor, V data, ByteComparable.Version version)
    {
        if (accessor.isEmpty(data))
            return null;

        V[] bufs = split(accessor, data);
        int lengthWithoutTrailingNulls = 0;
        for (int i = 0; i < bufs.length; ++i)
            if (bufs[i] != null)
                lengthWithoutTrailingNulls = i + 1;

        ByteSource[] srcs = new ByteSource[lengthWithoutTrailingNulls];
        for (int i = 0; i < lengthWithoutTrailingNulls; ++i)
            srcs[i] = bufs[i] != null ? subTypes.get(i).asComparableBytes(accessor, bufs[i], version) : null;

        // Because we stop early when there are trailing nulls, there needs to be an explicit terminator to make the
        // type prefix-free.
        return ByteSource.withTerminator(ByteSource.TERMINATOR, srcs);
    }

    @Override
    public <V> V fromComparableBytes(ValueAccessor<V> accessor, ByteSource.Peekable comparableBytes, ByteComparable.Version version)
    {
        assert version == ByteComparable.Version.OSS50; // Reverse translation is not supported for the legacy version.
        if (comparableBytes == null)
            return accessor.empty();

        V[] componentBuffers = accessor.createArray(subTypes.size());
        for (int i = 0; i < subTypes.size(); ++i)
        {
            if (comparableBytes.peek() == ByteSource.TERMINATOR)
                break;  // the rest of the fields remain null
            AbstractType<?> componentType = subTypes.get(i);
            ByteSource.Peekable component = ByteSourceInverse.nextComponentSource(comparableBytes);
            if (component != null)
                componentBuffers[i] = componentType.fromComparableBytes(accessor, component, version);
            else
                componentBuffers[i] = null;
        }
        // consume terminator
        int terminator = comparableBytes.next();
        assert terminator == ByteSource.TERMINATOR : String.format("Expected TERMINATOR (0x%2x) after %d components",
                                                                   ByteSource.TERMINATOR,
                                                                   subTypes.size());
        return buildValue(accessor, componentBuffers);
    }

    /**
     * Split a tuple value into its component values.
     */
    public <V> V[] split(ValueAccessor<V> accessor, V value)
    {
        return split(accessor, value, size(), this);
    }

    /**
     * Split a tuple value into its component values.
     */
    public static <V> V[] split(ValueAccessor<V> accessor, V value, int numberOfElements, TupleType type)
    {
        V[] components = accessor.createArray(numberOfElements);
        int length = accessor.size(value);
        int position = 0;
        for (int i = 0; i < numberOfElements; i++)
        {
            if (position == length)
                return Arrays.copyOfRange(components, 0, i);

            if (position + 4 > length)
                throw new MarshalException(String.format("Not enough bytes to read %dth component", i));

            int size = accessor.getInt(value, position);
            position += 4;

            // size < 0 means null value
            if (size >= 0)
            {
                if (position + size > length)
                    throw new MarshalException(String.format("Not enough bytes to read %dth component", i));

                components[i] = accessor.slice(value, position, size);
                position += size;
            }
            else
                components[i] = null;
        }

        // error out if we got more values in the tuple/UDT than we expected
        if (position < length)
        {
            throw new MarshalException(String.format("Expected %s %s for %s column, but got more",
                                                     numberOfElements, numberOfElements == 1 ? "value" : "values",
                                                     type.asCQL3Type()));
        }

        return components;
    }

    @SafeVarargs
    public static <V> V buildValue(ValueAccessor<V> accessor, V... components)
    {
        int totalLength = 0;
        for (V component : components)
            totalLength += 4 + (component == null ? 0 : accessor.size(component));

        int offset = 0;
        V result = accessor.allocate(totalLength);
        for (V component : components)
        {
            if (component == null)
            {
                offset += accessor.putInt(result, offset, -1);

            }
            else
            {
                offset += accessor.putInt(result, offset, accessor.size(component));
                offset += accessor.copyTo(component, 0, result, accessor, offset, accessor.size(component));
            }
        }
        return result;
    }

    public static ByteBuffer buildValue(ByteBuffer... components)
    {
        return buildValue(ByteBufferAccessor.instance, components);
    }

    @Override
    public <V> String getString(V input, ValueAccessor<V> accessor)
    {
        if (input == null)
            return "null";

        StringBuilder sb = new StringBuilder();
        int offset = 0;
        for (int i = 0; i < size(); i++)
        {
            if (accessor.isEmptyFromOffset(input, offset))
                return sb.toString();

            if (i > 0)
                sb.append(":");

            AbstractType<?> type = type(i);
            int size = accessor.getInt(input, offset);
            offset += TypeSizes.INT_SIZE;
            if (size < 0)
            {
                sb.append("@");
                continue;
            }

            V field = accessor.slice(input, offset, size);
            offset += size;
            // We use ':' as delimiter, and @ to represent null, so escape them in the generated string
            String fld = COLON_PAT.matcher(type.getString(field, accessor)).replaceAll(ESCAPED_COLON);
            fld = AT_PAT.matcher(fld).replaceAll(ESCAPED_AT);
            sb.append(fld);
        }
        return sb.toString();
    }

    public ByteBuffer fromString(String source)
    {
        // Split the input on non-escaped ':' characters
        List<String> fieldStrings = AbstractCompositeType.split(source);

        if (fieldStrings.size() > size())
            throw new MarshalException(String.format("Invalid tuple literal: too many elements. Type %s expects %d but got %d",
                                                     asCQL3Type(), size(), fieldStrings.size()));

        ByteBuffer[] fields = new ByteBuffer[fieldStrings.size()];
        for (int i = 0; i < fieldStrings.size(); i++)
        {
            String fieldString = fieldStrings.get(i);
            // We use @ to represent nulls
            if (fieldString.equals("@"))
                continue;

            AbstractType<?> type = type(i);
            fieldString = ESCAPED_COLON_PAT.matcher(fieldString).replaceAll(COLON);
            fieldString = ESCAPED_AT_PAT.matcher(fieldString).replaceAll(AT);
            fields[i] = type.fromString(fieldString);
        }
        return buildValue(fields);
    }

    @Override
    public Term fromJSONObject(Object parsed) throws MarshalException
    {
        if (parsed instanceof String)
            parsed = JsonUtils.decodeJson((String) parsed);

        if (!(parsed instanceof List))
            throw new MarshalException(String.format(
                    "Expected a list representation of a tuple, but got a %s: %s", parsed.getClass().getSimpleName(), parsed));

        List<?> list = (List<?>) parsed;

        if (list.size() > subTypes.size())
            throw new MarshalException(String.format("Tuple contains extra items (expected %s): %s", subTypes.size(), parsed));
        else if (subTypes.size() > list.size())
            throw new MarshalException(String.format("Tuple is missing items (expected %s): %s", subTypes.size(), parsed));

        List<Term> terms = new ArrayList<>(list.size());
        Iterator<AbstractType<?>> typeIterator = subTypes.iterator();
        for (Object element : list)
        {
            if (element == null)
            {
                typeIterator.next();
                terms.add(Constants.NULL_VALUE);
            }
            else
            {
                terms.add(typeIterator.next().fromJSONObject(element));
            }
        }

        return new Tuples.DelayedValue(this, terms);
    }

    @Override
    public String toJSONString(ByteBuffer buffer, ProtocolVersion protocolVersion)
    {
        ByteBuffer duplicated = buffer.duplicate();
        int offset = 0;
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < subTypes.size(); i++)
        {
            if (i > 0)
                sb.append(", ");

            ByteBuffer value = CollectionSerializer.readValue(duplicated, ByteBufferAccessor.instance, offset);
            offset += CollectionSerializer.sizeOfValue(value, ByteBufferAccessor.instance);
            if (value == null)
                sb.append("null");
            else
                sb.append(subTypes.get(i).toJSONString(value, protocolVersion));
        }
        return sb.append("]").toString();
    }

    public TypeSerializer<ByteBuffer> getSerializer()
    {
        return serializer;
    }

    @Override
    protected boolean isCompatibleWithFrozen(MultiCellCapableType<?> previous)
    {
        if (!(previous instanceof TupleType))
            return false;

        return isSubTypesCompatibleWith(previous, AbstractType::isCompatibleWith);
    }

    @Override
    protected boolean isCompatibleWithMultiCell(MultiCellCapableType<?> previous)
    {
        if (!(previous instanceof TupleType))
            return false;

        return isSubTypesCompatibleWith(previous, AbstractType::isSerializationCompatibleWith);
    }

    @Override
    protected boolean isValueCompatibleWithFrozen(MultiCellCapableType<?> previous)
    {
        if (!(previous instanceof TupleType))
            return false;

        return isSubTypesCompatibleWith(previous, AbstractType::isValueCompatibleWith);
    }

    @Override
    public CQL3Type asCQL3Type()
    {
        return CQL3Type.Tuple.create(this);
    }

    @Override
    public String toString(boolean ignoreFreezing)
    {
        boolean includeFrozenType = !ignoreFreezing && !isMultiCell();

        StringBuilder sb = new StringBuilder();
        if (includeFrozenType)
            sb.append(FrozenType.class.getName()).append('(');
        sb.append(getClass().getName());
        // FrozenType applies to anything nested (it wouldn't make sense otherwise) and so we only put once at the
        // highest level. So we can ignore freezing in the subtypes if either we're already within a frozen type
        // (we're a sub-type ourselves and frozenType has been included at the outer level), or we're frozen.
        sb.append(stringifyTypeParameters(ignoreFreezing || !isMultiCell()));
        if (includeFrozenType)
            sb.append(')');
        return sb.toString();
    }

    protected String stringifyTypeParameters(boolean ignoreFreezing)
    {
        return TypeParser.stringifyTypeParameters(subTypes, ignoreFreezing);
    }

    @Override
    public ByteBuffer getMaskedValue()
    {
        ByteBuffer[] buffers = new ByteBuffer[subTypes.size()];
        for (int i = 0; i < subTypes.size(); i++)
        {
            AbstractType<?> type = subTypes.get(i);
            buffers[i] = type.getMaskedValue();
        }

        return serializer.serialize(buildValue(buffers));
    }
}

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

import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Streams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.AssignmentTestable;
import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.cql3.functions.ArgumentDeserializer;
import org.apache.cassandra.cql3.statements.schema.AlterTableStatement;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.exceptions.InvalidColumnTypeException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;
import org.github.jamm.Unmetered;

import static com.google.common.collect.Iterables.transform;
import static org.apache.cassandra.db.marshal.AbstractType.ComparisonType.CUSTOM;

/**
 * Specifies a Comparator for a specific type of ByteBuffer.
 * <p>
 * Note that empty ByteBuffer are used to represent "start at the beginning"
 * or "stop at the end" arguments to get_slice, so the Comparator
 * should always handle those values even if they normally do not
 * represent a valid ByteBuffer for the type being compared.
 */
@Unmetered
public abstract class AbstractType<T> implements Comparator<ByteBuffer>, AssignmentTestable
{
    private final static Logger logger = LoggerFactory.getLogger(AbstractType.class);

    private final static int VARIABLE_LENGTH = -1;

    public enum ComparisonType
    {
        /**
         * This type should never be compared
         */
        NOT_COMPARABLE,
        /**
         * This type is always compared by its sequence of unsigned bytes
         */
        BYTE_ORDER,
        /**
         * This type can only be compared by calling the type's compareCustom() method, which may be expensive.
         * Support for this may be removed in a major release of Cassandra, however upgrade facilities will be
         * provided if and when this happens.
         */
        CUSTOM
    }

    public final ComparisonType comparisonType;
    public final boolean isByteOrderComparable;
    public final ValueComparators comparatorSet;
    public final boolean isMultiCell;
    public final ImmutableList<AbstractType<?>> subTypes;

    private final int hashCode;

    protected AbstractType(ComparisonType comparisonType)
    {
        this(comparisonType, false, ImmutableList.of());
    }

    protected AbstractType(ComparisonType comparisonType, boolean isMultiCell, ImmutableList<AbstractType<?>> subTypes)
    {
        this.isMultiCell = isMultiCell;
        this.comparisonType = comparisonType;
        this.isByteOrderComparable = comparisonType == ComparisonType.BYTE_ORDER;

        // A frozen type can only have frozen subtypes, basically by definition. So make sure we don't mess it up
        // when constructing types by forgetting to set some multi-cell flag.
        if (!isMultiCell)
        {
            if (Iterables.any(subTypes, AbstractType::isMultiCell))
                this.subTypes = ImmutableList.copyOf(Iterables.transform(subTypes, AbstractType::freeze));
            else
                this.subTypes = subTypes;
        }
        else
        {
            this.subTypes = subTypes;
        }
        if (subTypes != this.subTypes)
            logger.warn("Detected corrupted type: creating a frozen {} but with some non-frozen subtypes {}. " +
                        "This is likely a bug and should be reported.",
                        getClass(),
                        subTypes.stream().filter(AbstractType::isMultiCell).map(AbstractType::toString).collect(Collectors.joining(", ")));

        try
        {
            Method custom = getClass().getMethod("compareCustom", Object.class, ValueAccessor.class, Object.class, ValueAccessor.class);
            if ((custom.getDeclaringClass() == AbstractType.class) == (comparisonType == CUSTOM))
                throw new IllegalStateException((comparisonType == CUSTOM ? "compareCustom must be overridden if ComparisonType is CUSTOM"
                                                                         : "compareCustom should not be overridden if ComparisonType is not CUSTOM")
                                                + " (" + getClass().getSimpleName() + ")");
        }
        catch (NoSuchMethodException e)
        {
            throw new IllegalStateException();
        }

        comparatorSet = new ValueComparators((l, r) -> compare(l, ByteArrayAccessor.instance, r, ByteArrayAccessor.instance),
                                             (l, r) -> compare(l, ByteBufferAccessor.instance, r, ByteBufferAccessor.instance));

        hashCode = Objects.hash(getClass(), this.isMultiCell, this.subTypes);
    }

    static <VL, VR, T extends Comparable<T>> int compareComposed(VL left, ValueAccessor<VL> accessorL, VR right, ValueAccessor<VR> accessorR, AbstractType<T> type)
    {
        if (accessorL.isEmpty(left) || accessorR.isEmpty(right))
            return Boolean.compare(accessorR.isEmpty(right), accessorL.isEmpty(left));

        return type.compose(left, accessorL).compareTo(type.compose(right, accessorR));
    }

    public static List<String> asCQLTypeStringList(List<AbstractType<?>> abstractTypes)
    {
        List<String> r = new ArrayList<>(abstractTypes.size());
        for (AbstractType<?> abstractType : abstractTypes)
            r.add(abstractType.asCQL3Type().toString());
        return r;
    }

    public final T compose(ByteBuffer bytes)
    {
        return getSerializer().deserialize(bytes);
    }

    public <V> T compose(V value, ValueAccessor<V> accessor)
    {
        return getSerializer().deserialize(value, accessor);
    }

    public ByteBuffer decomposeUntyped(Object value)
    {
        return decompose((T) value);
    }

    public ByteBuffer decompose(T value)
    {
        return getSerializer().serialize(value);
    }

    /** get a string representation of the bytes used for various identifier (NOT just for log messages) */
    public <V> String getString(V value, ValueAccessor<V> accessor)
    {
        if (value == null)
            return "null";

        TypeSerializer<T> serializer = getSerializer();
        serializer.validate(value, accessor);

        return serializer.toString(serializer.deserialize(value, accessor));
    }

    public final String getString(ByteBuffer bytes)
    {
        return getString(bytes, ByteBufferAccessor.instance);
    }

    public String toCQLString(ByteBuffer bytes)
    {
        return asCQL3Type().toCQLLiteral(bytes);
    }

    /** get a byte representation of the given string. */
    public abstract ByteBuffer fromString(String source) throws MarshalException;

    /** Given a parsed JSON string, return a byte representation of the object.
     * @param parsed the result of parsing a json string
     **/
    public abstract Term fromJSONObject(Object parsed) throws MarshalException;

    /**
     * Converts the specified value into its JSON representation.
     * <p>
     * The buffer position will stay the same.
     * </p>
     *
     * @param buffer the value to convert
     * @param protocolVersion the protocol version to use for the conversion
     * @return a JSON string representing the specified value
     */
    public String toJSONString(ByteBuffer buffer, ProtocolVersion protocolVersion)
    {
        return '"' + Objects.toString(getSerializer().deserialize(buffer), "") + '"';
    }

    public <V> String toJSONString(V value, ValueAccessor<V> accessor, ProtocolVersion protocolVersion)
    {
        return toJSONString(accessor.toBuffer(value), protocolVersion); // FIXME
    }

    /* validate that the byte array is a valid sequence for the type we are supposed to be comparing */
    public void validate(ByteBuffer bytes) throws MarshalException
    {
        validate(bytes, ByteBufferAccessor.instance);
    }

    public <V> void validate(V value, ValueAccessor<V> accessor) throws MarshalException
    {
        getSerializer().validate(value, accessor);
    }

    public final int compare(ByteBuffer left, ByteBuffer right)
    {
        return compare(left, ByteBufferAccessor.instance, right, ByteBufferAccessor.instance);
    }

    public final <VL, VR> int compare(VL left, ValueAccessor<VL> accessorL, VR right, ValueAccessor<VR> accessorR)
    {
        return isByteOrderComparable ? ValueAccessor.compare(left, accessorL, right, accessorR) : compareCustom(left, accessorL, right, accessorR);
    }

    /**
     * Implement IFF ComparisonType is CUSTOM
     *
     * Compares the byte representation of two instances of this class,
     * for types where this cannot be done by simple in-order comparison of the
     * unsigned bytes
     *
     * Standard Java compare semantics
     * @param left
     * @param accessorL
     * @param right
     * @param accessorR
     */
    public <VL, VR> int compareCustom(VL left, ValueAccessor<VL> accessorL, VR right, ValueAccessor<VR> accessorR)
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Validate cell value. Unlike {@linkplain #validate(java.nio.ByteBuffer)},
     * cell value is passed to validate its content.
     * Usually, this is the same as validate except collection.
     *
     * @param cellValue ByteBuffer representing cell value
     * @throws MarshalException
     */
    public <V> void validateCellValue(V cellValue, ValueAccessor<V> accessor) throws MarshalException
    {
        validate(cellValue, accessor);
    }

    /* Most of our internal type should override that. */
    public CQL3Type asCQL3Type()
    {
        return new CQL3Type.Custom(this);
    }

    public AbstractType<?> udfType()
    {
        return this;
    }

    /**
     * Same as compare except that this ignore ReversedType. This is to be use when
     * comparing 2 values to decide for a CQL condition (see Operator.isSatisfiedBy) as
     * for CQL, ReversedType is simply an "hint" to the storage engine but it does not
     * change the meaning of queries per-se.
     */
    public int compareForCQL(ByteBuffer v1, ByteBuffer v2)
    {
        return compare(v1, v2);
    }

    /**
     * Returns the serializer for this type.
     * Note that the method must return a different instance of serializer for different types even if the types
     * use the same serializer - in this case, the method should return separate instances for which equals() returns
     * false.
     */
    public abstract TypeSerializer<T> getSerializer();

    /**
     * @return the deserializer used to deserialize the function arguments of this type.
     */
    public ArgumentDeserializer getArgumentDeserializer()
    {
        return new DefaultArgumentDeserializer(this);
    }

    public boolean isCounter()
    {
        return false;
    }

    public boolean isReversed()
    {
        return false;
    }

    public AbstractType<T> unwrap()
    {
        return isReversed() ? ((ReversedType<T>) this).baseType.unwrap() : this;
    }

    public boolean isList()
    {
        return false;
    }

    public static AbstractType<?> parseDefaultParameters(AbstractType<?> baseType, TypeParser parser) throws SyntaxException
    {
        Map<String, String> parameters = parser.getKeyValueParameters();
        String reversed = parameters.get("reversed");
        if (reversed != null && (reversed.isEmpty() || reversed.equals("true")))
        {
            return ReversedType.getInstance(baseType);
        }
        else
        {
            return baseType;
        }
    }

    /**
     * Returns true if this comparator is compatible with the provided previous comparator, that is if previous can
     * safely be replaced by this.
     * A comparator cn should be compatible with a previous one cp if forall columns c1 and c2,
     * if   cn.validate(c1) and cn.validate(c2) and cn.compare(c1, c2) == v,
     * then cp.validate(c1) and cp.validate(c2) and cp.compare(c1, c2) == v.
     * <p/>
     * Note that a type should be compatible with at least itself and when in doubt, keep the default behavior
     * of not being compatible with any other comparator!
     * <p/>
     * Used for user functions and aggregates to validate the returning type when the function is replaced.
     * Used for validation of table metadata when replacing metadata in ref (alterting a table) and when scrubbing
     * an sstable to validate whether metadata stored in the sstable is compatible with the current metadata.
     * <p/>
     * Note that this will never return true when one type is multicell and the other is not.
     */
    public boolean isCompatibleWith(AbstractType<?> previous)
    {
        return this.equals(previous);
    }

    /**
     * Returns true if values of the other AbstractType can be read and "reasonably" interpreted by this
     * AbstractType. Note that this is a weaker version of isCompatibleWith, as it does not require that both type
     * compare values the same way.
     * <p/>
     * The restriction on the other type being "reasonably" interpreted is to prevent, for example, IntegerType from
     * being compatible with all other types.  Even though any byte string is a valid IntegerType value, it doesn't
     * necessarily make sense to interpret a UUID or a UTF8 string as an integer.
     * <p/>
     * Note that a type should be compatible with at least itself.
     * <p/>
     * Also note that to ensure consistent handling of the {@link ReversedType} (which should be ignored as far as this
     * method goes since it only impacts sorting), this method is final and subclasses should override the
     * {@link #isValueCompatibleWithInternal} method instead.
     * <p/>
     * Used for type casting and values assignment. It valid if we can compose L values which were decomposed using R
     * serializer. Therefore, it does not care about whether the type is reversed or not. It should not whether the
     * type is fixed or variable length as for compose/decompose we always deal with all remaining data in the buffer
     * (so for example, a variable length type may be compatible with fixed length type given the interpretation is
     * consistent, like between BigInt and Long).
     */
    public final boolean isValueCompatibleWith(AbstractType<?> previous)
    {
        if (previous == null)
            return false;

        AbstractType<T> unwrapped = this.unwrap();
        AbstractType<?> previousUnwrapped = previous.unwrap();
        if (unwrapped.equals(previousUnwrapped))
            return true;

        return unwrapped.isValueCompatibleWithInternal(previousUnwrapped);
    }

    /**
     * Needed to handle {@link ReversedType} in value-compatibility checks. Subclasses should override this instead of
     * {@link #isValueCompatibleWith}. However, if said override has subtypes on which they need to check value
     * compatibility recursively, they should call {@link #isValueCompatibleWith} instead of this method
     * so that reversed types are ignored even if nested.
     */
    protected boolean isValueCompatibleWithInternal(AbstractType<?> previous)
    {
        return isCompatibleWith(previous);
    }

    /**
     * Similar to {@link #isValueCompatibleWith(AbstractType)}, but takes into account {@link Cell} encoding.
     * In particular, this method doesn't consider two types serialization compatible if one of them has fixed
     * length (overrides {@link #valueLengthIfFixed()}, and the other one doesn't.
     * </p>
     * Used in {@link AlterTableStatement} when adding a column with the same name as the previously dropped column.
     * The new column type must be serialization compatible with the old one. We must be able to read cells of the new
     * type which were serialized as cells of the old type.
     */
    public boolean isSerializationCompatibleWith(AbstractType<?> previous)
    {
        return isValueCompatibleWith(previous)
               && valueLengthIfFixed() == previous.valueLengthIfFixed()
               && isMultiCell() == previous.isMultiCell();
    }

    /**
     * An alternative comparison function used by CollectionsType in conjunction with CompositeType.
     *
     * This comparator is only called to compare components of a CompositeType. It gets the value of the
     * previous component as argument (or null if it's the first component of the composite).
     *
     * Unless you're doing something very similar to CollectionsType, you shouldn't override this.
     */
    public <VL, VR> int compareCollectionMembers(VL left, ValueAccessor<VL> accessorL, VR right, ValueAccessor<VR> accessorR, VL collectionName)
    {
        return compare(left, accessorL, right, accessorR);
    }

    public <V> void validateCollectionMember(V value, V collectionName, ValueAccessor<V> accessor) throws MarshalException
    {
        getSerializer().validate(value, accessor);
    }

    public boolean isCollection()
    {
        return false;
    }

    public boolean isUDT()
    {
        return false;
    }

    public boolean isTuple()
    {
        return false;
    }

    public boolean isVector()
    {
        return false;
    }

    public final boolean isMultiCell()
    {
        return isMultiCell;
    }

    /**
     * If the type is a multi-cell one ({@link #isMultiCell()} is true), returns a frozen copy of this type (one
     * for which {@link #isMultiCell()} returns false).
     * <p>
     * Note that as mentioned on {@link #isMultiCell()}, a frozen type necessarily has all its subtypes frozen, so
     * this method also ensures that no subtypes (recursively) are marked as multi-cell.
     *
     * @return a frozen version of this type. If this type is not multi-cell (whether because it is not a "complex"
     * type, or because it is already a frozen one), this should return {@code this}.
     */
    public AbstractType<?> freeze()
    {
        if (!isMultiCell())
            return this;

        return with(freeze(subTypes()), false);
    }

    /**
     * Creates an instance of this type (the concrete type extending this class) with the provided updated multi-cell
     * flag and subtypes.
     * <p>
     * Any other information (other than multi-cellness and subtypes) the type may have is expected to be left unchanged
     * in the created type.
     *
     * @param isMultiCell whether the returned type must be a multi-cell one or not.
     * @param subTypes the subtypes to use for the returned type as a list. The list will have subtypes in the exact
     * same order as returned by {@link #subTypes()}, and exactly as many as the concrete class expects.
     * @return the created type, which can be {@code this} if the provided subTypes and multi-cell flag are the same
     * as that of this type.
     */
    public AbstractType<T> with(ImmutableList<AbstractType<?>> subTypes, boolean isMultiCell)
    {
        // Default implementation for types that can neither be multi-cell, nor have subtypes (and thus where this
        // is basically a no-op). Any other type must override this.

        assert this.subTypes.isEmpty() && subTypes.isEmpty() :
        String.format("Invalid call to 'with' on %s with subTypes %s (provided subTypes: %s)",
                      this, this.subTypes, subTypes);

        assert !this.isMultiCell() && !isMultiCell:
        String.format("Invalid call to 'with' on %s with isMultiCell %b (provided isMultiCell: %b)",
                      this, this.isMultiCell(), isMultiCell);

        return this;
    }

    /**
     * If the type has "complex" values that depend on subtypes, return those (direct) subtypes (in undefined order),
     * and an empty list otherwise.
     */
    public final ImmutableList<AbstractType<?>> subTypes()
    {
        return subTypes;
    }

    /**
     * Returns {@code true} for types where empty should be handled like {@code null} like {@link Int32Type}.
     */
    public boolean isEmptyValueMeaningless()
    {
        return false;
    }

    /**
     * @param ignoreFreezing if true, the type string will not be wrapped with FrozenType(...), even if this type is frozen.
     */
    public String toString(boolean ignoreFreezing)
    {
        return getClass().getName();
    }

    /**
     * To override keyspace name in {@link UserType}
     */
    public AbstractType<T> overrideKeyspace(Function<String, String> overrideKeyspace)
    {
        if (subTypes.isEmpty())
            return this;
        else
            return with(subTypes.stream().map(t -> t.overrideKeyspace(overrideKeyspace)).collect(ImmutableList.toImmutableList()), isMultiCell);
    }

    /**
     * The length of values for this type, in bytes, if all values are of fixed length, -1 otherwise.
     * This has an impact on serialization.
     * <lu>
     *  <li> see {@link #writeValue} </li>
     *  <li> see {@link #read} </li>
     *  <li> see {@link #writtenLength} </li>
     *  <li> see {@link #skipValue} </li>
     * </lu>
     */
    public int valueLengthIfFixed()
    {
        return VARIABLE_LENGTH;
    }

    /**
     * Checks if all values are of fixed length.
     *
     * @return {@code true} if all values are of fixed length, {@code false} otherwise.
     */
    public final boolean isValueLengthFixed()
    {
        return valueLengthIfFixed() != VARIABLE_LENGTH;
    }

    /**
     * Defines if the type allows an empty set of bytes ({@code new byte[0]}) as valid input.  The {@link #validate(Object, ValueAccessor)}
     * and {@link #compose(Object, ValueAccessor)} methods must allow empty bytes when this returns true, and must reject empty bytes
     * when this is false.
     * <p/>
     * As of this writing, the main user of this API is for testing to know what types allow empty values and what types don't,
     * so that the data that gets generated understands when {@link ByteBufferUtil#EMPTY_BYTE_BUFFER} is allowed as valid data.
     */
    public boolean allowsEmpty()
    {
        return false;
    }

    public boolean isNull(ByteBuffer bb)
    {
        return isNull(bb, ByteBufferAccessor.instance);
    }

    public <V> boolean isNull(V buffer, ValueAccessor<V> accessor)
    {
        return getSerializer().isNull(buffer, accessor);
    }

    // This assumes that no empty values are passed
    public void writeValue(ByteBuffer value, DataOutputPlus out) throws IOException
    {
        writeValue(value, ByteBufferAccessor.instance, out);
    }

    // This assumes that no empty values are passed
    public  <V> void writeValue(V value, ValueAccessor<V> accessor, DataOutputPlus out) throws IOException
    {
        assert !isNull(value, accessor) : "bytes should not be null for type " + this;
        int expectedValueLength = valueLengthIfFixed();
        if (expectedValueLength >= 0)
        {
            int actualValueLength = accessor.size(value);
            if (actualValueLength == expectedValueLength)
                accessor.write(value, out);
            else
                throw new IOException(String.format("Expected exactly %d bytes, but was %d",
                                                    expectedValueLength, actualValueLength));
        }
        else
        {
            accessor.writeWithVIntLength(value, out);
        }
    }

    public long writtenLength(ByteBuffer value)
    {
        return writtenLength(value, ByteBufferAccessor.instance);
    }

    public <V> long writtenLength(V value, ValueAccessor<V> accessor)
    {
        assert !accessor.isEmpty(value) : "bytes should not be empty for type " + this;
        return valueLengthIfFixed() >= 0
               ? accessor.size(value) // if the size is wrong, this will be detected in writeValue
               : accessor.sizeWithVIntLength(value);
    }

    public ByteBuffer readBuffer(DataInputPlus in) throws IOException
    {
        return readBuffer(in, Integer.MAX_VALUE);
    }

    public ByteBuffer readBuffer(DataInputPlus in, int maxValueSize) throws IOException
    {
        return read(ByteBufferAccessor.instance, in, maxValueSize);
    }

    public byte[] readArray(DataInputPlus in, int maxValueSize) throws IOException
    {
        return read(ByteArrayAccessor.instance, in, maxValueSize);
    }

    public <V> V read(ValueAccessor<V> accessor, DataInputPlus in, int maxValueSize) throws IOException
    {
        int length = valueLengthIfFixed();

        if (length >= 0)
            return accessor.read(in, length);
        else
        {
            int l = in.readUnsignedVInt32();
            if (l < 0)
                throw new IOException("Corrupt (negative) value length encountered");

            if (l > maxValueSize)
                throw new IOException(String.format("Corrupt value length %d encountered, as it exceeds the maximum of %d, " +
                                                    "which is set via max_value_size in cassandra.yaml",
                                                    l, maxValueSize));

            return accessor.read(in, l);
        }
    }

    public void skipValue(DataInputPlus in) throws IOException
    {
        int length = valueLengthIfFixed();
        if (length >= 0)
            in.skipBytesFully(length);
        else
            ByteBufferUtil.skipWithVIntLength(in);
    }

    public final boolean referencesUserType(ByteBuffer name)
    {
        return referencesUserType(name, ByteBufferAccessor.instance);
    }

    /**
     * Returns true if this type is or references a user type with provided name.
     */
    public <V> boolean referencesUserType(V name, ValueAccessor<V> accessor)
    {
        // Note that non-complex types have no subtypes, so will return false, and UserType overrides this to return
        // true if the provided name matches.
        return subTypes().stream().anyMatch(t -> t.referencesUserType(name, accessor));
    }

    /**
     * Whether this type is or contains any UDT.
     */
    public final boolean referencesUserTypes()
    {
        return isUDT() || subTypes().stream().anyMatch(AbstractType::referencesUserTypes);
    }

    /**
     * Returns an instance of this type with all references to the provided user type recursively replaced with its new
     * definition.
     */
    public AbstractType<?> withUpdatedUserType(UserType udt)
    {
        if (!referencesUserType(udt.name))
            return this;

        ImmutableList.Builder<AbstractType<?>> builder = ImmutableList.builder();
        for (AbstractType<?> subType : subTypes)
            builder.add(subType.withUpdatedUserType(udt));

        return with(builder.build(), isMultiCell());
    }

    /**
     * Returns an instance of this type with all references to the provided user types recursively replaced with their new
     * definition.
     */
    public final AbstractType<?> withUpdatedUserTypes(Iterable<UserType> udts)
    {
        if (!referencesUserTypes())
            return this;

        AbstractType<?> type = this;
        for (UserType udt : udts)
            type = type.withUpdatedUserType(udt);

        return type;
    }

    /**
     * Replace any instances of UserType with equivalent TupleType-s.
     * <p>
     * We need it for dropped_columns, to allow safely dropping unused user types later without retaining any references
     * to them in system_schema.dropped_columns.
     */
    public AbstractType<?> expandUserTypes()
    {
        return referencesUserTypes()
               ? with(ImmutableList.copyOf(transform(subTypes, AbstractType::expandUserTypes)), isMultiCell())
               : this;
    }

    public boolean referencesDuration()
    {
        // Note that non-complex types have no subtypes, so will return false, and DurationType overrides this to return
        // true.
        return subTypes().stream().anyMatch(AbstractType::referencesDuration);
    }

    public final boolean referencesCounter()
    {
        return isCounter() || subTypes().stream().anyMatch(AbstractType::referencesCounter);
    }

    /**
     * Tests whether a CQL value having this type can be assigned to the provided receiver.
     */
    public AssignmentTestable.TestResult testAssignment(AbstractType<?> receiverType)
    {
        // testAssignement is for CQL literals and native protocol values, none of which make a meaningful
        // difference between frozen or not and reversed or not.

        if (!isMultiCell())
            receiverType = receiverType.freeze();

        if (isReversed() && !receiverType.isReversed())
            receiverType = ReversedType.getInstance(receiverType);

        if (equals(receiverType))
            return AssignmentTestable.TestResult.EXACT_MATCH;

        if (receiverType.isValueCompatibleWith(this))
            return AssignmentTestable.TestResult.WEAKLY_ASSIGNABLE;

        return AssignmentTestable.TestResult.NOT_ASSIGNABLE;
    }

    /**
     * Validates whether this type is valid as a column type for a column of the provided kind.
     * <p>
     * A number of limits must be respected by column types (possibly depending on the type of columns). For
     * instance, primary key columns must always be frozen, cannot use counters, etc. And for regular columns, amongst
     * other things, we currently only support non-frozen types at top-level, so any type with a non-frozen subtype
     * is invalid (note that it's valid to <b>create</b> a type with non-frozen subtypes, with a {@code CREATE TYPE}
     * for instance, but they cannot be used as column types without being frozen).
     *
     * @param columnName         the name of the column whose type is checked.
     * @param isPrimaryKeyColumn whether {@code columnName} is a primary key column or not.
     * @param isCounterTable     whether the table the {@code columnName} is part of is a counter table.
     * @throws InvalidColumnTypeException if this type is not a valid column type for {@code columnName}.
     */
    public void validateForColumn(ByteBuffer columnName,
                                  boolean isPrimaryKeyColumn,
                                  boolean isCounterTable,
                                  boolean isDroppedColumn,
                                  boolean isForOfflineTool)
    {
        if (isPrimaryKeyColumn)
        {
            if (isMultiCell())
                throw columnException(columnName,
                                      "non-frozen %s are not supported for PRIMARY KEY columns", category());
            if (referencesCounter())
                throw columnException(columnName,
                                      "counters are not supported within PRIMARY KEY columns");

            // We don't allow durations in anything sorted (primary key here, or in the "name-comparator" part of
            // collections below). This isn't really a technical limitation, but duration sorts in a somewhat random
            // way, so CASSANDRA-11873 decided to reject them when sorting was involved.
            if (referencesDuration())
                throw columnException(columnName,
                                      "duration types are not supported within PRIMARY KEY columns");

            if (comparisonType == ComparisonType.NOT_COMPARABLE)
                throw columnException(columnName,
                                      "type %s is not comparable and cannot be used for PRIMARY KEY columns", asCQL3Type().toSchemaString());
        }
        else
        {
            if (isMultiCell())
            {
                if (isTuple() && !isDroppedColumn && !isForOfflineTool)
                    throw columnException(columnName,
                                          "tuple type %s is not frozen, which should not have happened",
                                          asCQL3Type().toSchemaString());

                for (AbstractType<?> subType : subTypes())
                {
                    if (subType.isMultiCell())
                    {
                        throw columnException(columnName,
                                              "non-frozen %s are only supported at top-level: subtype %s of %s must be frozen",
                                              subType.category(), subType.asCQL3Type().toSchemaString(), asCQL3Type().toSchemaString());
                    }
                }

                if (this instanceof MultiCellCapableType)
                {
                    AbstractType<?> nameComparator = ((MultiCellCapableType<?>) this).nameComparator();
                    // As mentioned above, CASSANDRA-11873 decided to reject durations when sorting was involved.
                    if (nameComparator.referencesDuration())
                    {
                        // Trying to profile a more precise error message
                        String what = this instanceof MapType
                                      ? "map keys"
                                      : (this instanceof SetType ? "sets" : category());
                        throw columnException(columnName, "duration types are not supported within non-frozen %s", what);
                    }
                }
            }

            // Mixing counter with non counter columns is not supported (#2614)
            if (isCounterTable)
            {
                // Everything within a counter table must be a counter, and we don't allow nesting (collections of
                // counters), except for legacy backward-compatibility, in the super-column map used to support old
                // super columns.
                if (!isCounter() && !TableMetadata.isSuperColumnMapColumnName(columnName))
                {
                    // We don't allow counter inside collections, but to be fair, at least for map, it's a bit of an
                    // arbitrary limitation (it works internally, we don't expose it mostly because counters have
                    // their limitations, and we want to restrict how user can use them to hopefully make user think
                    // twice about their usage). In any case, a slightly more user-friendly message is probably nice.
                    if (referencesCounter())
                        throw columnException(columnName, "counters are not allowed within %s", category());

                    throw columnException(columnName, "Cannot mix counter and non counter columns in the same table");
                }
            }
            else
            {
                if (isCounter())
                    throw columnException(columnName, "Cannot mix counter and non counter columns in the same table");

                // For nested counters, we prefer complaining about the nested-ness rather than this not being a counter
                // table, because the table won't be marked as a counter one even if it has only nested counters, and so
                // that's overall a more intuitive message.
                if (referencesCounter())
                    throw columnException(columnName, "counters are not allowed within %s", category());
            }
        }

    }

    private InvalidColumnTypeException columnException(ByteBuffer columnName,
                                                       String reason,
                                                       Object... args)
    {
        String msg = args.length == 0 ? reason : String.format(reason, args);
        return new InvalidColumnTypeException(columnName, this, msg);
    }

    private String category()
    {
        if (isCollection())
            return "collections";
        else if (isTuple())
            return "tuples";
        else if (isUDT())
            return "user types";
        else
            return "types";
    }

    /**
     * Produce a byte-comparable representation of the given value, i.e. a sequence of bytes that compares the same way
     * using lexicographical unsigned byte comparison as the original value using the type's comparator.
     *
     * We use a slightly stronger requirement to be able to use the types in tuples. Precisely, for any pair x, y of
     * non-equal valid values of this type and any bytes b1, b2 between 0x10 and 0xEF,
     * (+ stands for concatenation)
     *   compare(x, y) == compareLexicographicallyUnsigned(asByteComparable(x)+b1, asByteComparable(y)+b2)
     * (i.e. the values compare like the original type, and an added 0x10-0xEF byte at the end does not change that) and:
     *   asByteComparable(x)+b1 is not a prefix of asByteComparable(y)      (weakly prefix free)
     * (i.e. a valid representation of a value may be a prefix of another valid representation of a value only if the
     * following byte in the latter is smaller than 0x10 or larger than 0xEF). These properties are trivially true if
     * the encoding compares correctly and is prefix free, but also permits a little more freedom that enables somewhat
     * more efficient encoding of arbitrary-length byte-comparable blobs.
     *
     * Depending on the type, this method can be called for null or empty input, in which case the output is allowed to
     * be null (the clustering/tuple encoding will accept and handle it).
     */
    public <V> ByteSource asComparableBytes(ValueAccessor<V> accessor, V value, ByteComparable.Version version)
    {
        if (isByteOrderComparable)
        {
            // When a type is byte-ordered on its own, we only need to escape it, so that we can include it in
            // multi-component types and make the encoding weakly-prefix-free.
            return ByteSource.of(accessor, value, version);
        }
        else
            // default is only good for byte-comparables
            throw new UnsupportedOperationException(getClass().getSimpleName() + " does not implement asComparableBytes");
    }

    public final ByteSource asComparableBytes(ByteBuffer byteBuffer, ByteComparable.Version version)
    {
        return asComparableBytes(ByteBufferAccessor.instance, byteBuffer, version);
    }

    /**
     * Translates the given byte-ordered representation to the common, non-byte-ordered binary representation of a
     * payload for this abstract type (the latter, common binary representation is what we mostly work with in the
     * storage engine internals). If the given bytes don't correspond to the encoding of some payload value for this
     * abstract type, an {@link IllegalArgumentException} may be thrown.
     *
     * @param accessor value accessor used to construct the value.
     * @param comparableBytes A byte-ordered representation (presumably of a payload for this abstract type).
     * @param version The byte-comparable version used to construct the representation.
     * @return A of a payload for this abstract type, corresponding to the given byte-ordered representation,
     *         constructed using the supplied value accessor.
     *
     * @see #asComparableBytes
     */
    public <V> V fromComparableBytes(ValueAccessor<V> accessor, ByteSource.Peekable comparableBytes, ByteComparable.Version version)
    {
        if (isByteOrderComparable)
            return accessor.valueOf(ByteSourceInverse.getUnescapedBytes(comparableBytes));
        else
            throw new UnsupportedOperationException(getClass().getSimpleName() + " does not implement fromComparableBytes");
    }

    public final ByteBuffer fromComparableBytes(ByteSource.Peekable comparableBytes, ByteComparable.Version version)
    {
        return fromComparableBytes(ByteBufferAccessor.instance, comparableBytes, version);
    }

    /**
     * This must be overriden by subclasses if necessary so that for any
     * AbstractType, this == TypeParser.parse(toString()).
     *
     * Note that for backwards compatibility this includes the full classname.
     * For CQL purposes the short name is fine.
     */
    @Override
    public final String toString()
    {
        return toString(false);
    }

    public void checkComparable()
    {
        switch (comparisonType)
        {
            case NOT_COMPARABLE:
                throw new IllegalArgumentException(this + " cannot be used in comparisons, so cannot be used as a clustering column");
        }
    }

    public final AssignmentTestable.TestResult testAssignment(String keyspace, ColumnSpecification receiver)
    {
        return testAssignment(receiver.type);
    }

    @Override
    public AbstractType<?> getCompatibleTypeIfKnown(String keyspace)
    {
        return this;
    }

    /**
     * @return A fixed, serialized value to be used when the column is masked, to be returned instead of the real value.
     */
    public ByteBuffer getMaskedValue()
    {
        throw new UnsupportedOperationException("There isn't a defined masked value for type " + asCQL3Type());
    }

    protected static <K, V extends AbstractType<?>> V getInstance(ConcurrentMap<K, V> instances, K key, Supplier<V> value)
    {
        V cached = instances.get(key);
        if (cached != null)
            return cached;

        // We avoid constructor calls in Map#computeIfAbsent to avoid recursive update exceptions because the automatic
        // fixing of subtypes done by the top-level constructor might attempt a recursive update to the instances map.
        V instance = value.get();
        return instances.computeIfAbsent(key, k -> instance);
    }

    /**
     * Utility method that freezes a list of types.
     *
     * @param types the list of types to freeze.
     * @return a new (unmodifiable) list containing the result of applying {@link #freeze()} on every type of
     * {@code types}.
     */
    public static ImmutableList<AbstractType<?>> freeze(Iterable<AbstractType<?>> types)
    {
        if (Iterables.isEmpty(types))
            return ImmutableList.of();

        ImmutableList.Builder<AbstractType<?>> builder = ImmutableList.builder();
        for (AbstractType<?> type : types)
            builder.add(type.freeze());
        return builder.build();
    }

    /**
     * {@link ArgumentDeserializer} that uses the type deserialization.
     */
    protected static class DefaultArgumentDeserializer implements ArgumentDeserializer
    {
        private final AbstractType<?> type;

        public DefaultArgumentDeserializer(AbstractType<?> type)
        {
            this.type = type;
        }

        @Override
        public Object deserialize(ProtocolVersion protocolVersion, ByteBuffer buffer)
        {
            if (buffer == null || (!buffer.hasRemaining() && type.isEmptyValueMeaningless()))
                return null;

            return type.compose(buffer);
        }
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        if (this.hashCode() != o.hashCode())
            return false;
        AbstractType<?> that = (AbstractType<?>) o;
        return isMultiCell == that.isMultiCell && Objects.equals(subTypes, that.subTypes);
    }

    @Override
    public int hashCode()
    {
        return hashCode;
    }

    /**
     * Checks whether this type's subtypes are compatible with the provided type's subtypes using a provided predicate.
     * Regardless of the predicate, this method returns false if this type has fewer subtypes than the provided type
     * because in that case it could not safely replace the provided type in any situation.
     *
     * @param previous  the type against which the verification is done - in other words, the type which was originally
     *                  used to serialize the values
     * @param predicate one of the methodsd isXXXCompatibleWith
     * @return {@code true} if this type has at least the same number of subtypes as the previous type and the predicate
     * is satisfied for the corresponding subtypes
     */
    protected boolean isSubTypesCompatibleWith(AbstractType<?> previous, BiPredicate<AbstractType<?>, AbstractType<?>> predicate)
    {
        if (subTypes.size() < previous.subTypes.size())
            return false;

        return Streams.zip(subTypes.stream().limit(previous.subTypes.size()), previous.subTypes.stream(), predicate::test)
                      .allMatch(Predicate.isEqual(true));
    }

}

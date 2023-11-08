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
package org.apache.cassandra.cql3.restrictions;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.serializers.ListSerializer;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.cql3.statements.Bound;
import org.apache.cassandra.db.MultiCBuilder;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.IndexRegistry;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.cassandra.cql3.statements.RequestValidations.checkBindValueSet;
import static org.apache.cassandra.cql3.statements.RequestValidations.checkFalse;
import static org.apache.cassandra.cql3.statements.RequestValidations.checkTrue;
import static org.apache.cassandra.cql3.statements.RequestValidations.invalidRequest;

public abstract class SingleColumnRestriction implements SingleRestriction
{
    /**
     * The definition of the column to which apply the restriction.
     */
    protected final ColumnMetadata columnDef;

    public SingleColumnRestriction(ColumnMetadata columnDef)
    {
        this.columnDef = columnDef;
    }

    @Override
    public List<ColumnMetadata> getColumnDefs()
    {
        return Collections.singletonList(columnDef);
    }

    @Override
    public ColumnMetadata getFirstColumn()
    {
        return columnDef;
    }

    @Override
    public ColumnMetadata getLastColumn()
    {
        return columnDef;
    }

    @Override
    public boolean hasSupportingIndex(IndexRegistry indexRegistry)
    {
        for (Index index : indexRegistry.listIndexes())
            if (isSupportedBy(index))
                return true;

        return false;
    }

    @Override
    public Index findSupportingIndex(IndexRegistry indexRegistry)
    {
        for (Index index : indexRegistry.listIndexes())
            if (isSupportedBy(index))
                return index;

        return null;
    }

    @Override
    public boolean needsFiltering(Index.Group indexGroup)
    {
        for (Index index : indexGroup.getIndexes())
            if (isSupportedBy(index))
                return false;

        return true;
    }

    @Override
    public final SingleRestriction mergeWith(SingleRestriction otherRestriction)
    {
        // We want to allow query like: b > ? AND (b,c) < (?, ?)
        if (otherRestriction.isMultiColumn() && canBeConvertedToMultiColumnRestriction())
        {
            return toMultiColumnRestriction().mergeWith(otherRestriction);
        }

        return doMergeWith(otherRestriction);
    }

    protected abstract SingleRestriction doMergeWith(SingleRestriction otherRestriction);

    /**
     * Converts this <code>SingleColumnRestriction</code> into a {@link MultiColumnRestriction}
     *
     * @return the <code>MultiColumnRestriction</code> corresponding to this
     */
    abstract MultiColumnRestriction toMultiColumnRestriction();

    /**
     * Checks if this <code>Restriction</code> can be converted into a {@link MultiColumnRestriction}
     *
     * @return <code>true</code> if this <code>Restriction</code> can be converted into a
     * {@link MultiColumnRestriction}, <code>false</code> otherwise.
     */
    boolean canBeConvertedToMultiColumnRestriction()
    {
        return true;
    }

    /**
     * Check if this type of restriction is supported by the specified index.
     *
     * @param index the secondary index
     * @return <code>true</code> this type of restriction is supported by the specified index,
     * <code>false</code> otherwise.
     */
    protected abstract boolean isSupportedBy(Index index);

    public static final class EQRestriction extends SingleColumnRestriction
    {
        private final Term term;

        public EQRestriction(ColumnMetadata columnDef, Term term)
        {
            super(columnDef);
            this.term = term;
        }

        @Override
        public void addFunctionsTo(List<Function> functions)
        {
            term.addFunctionsTo(functions);
        }

        @Override
        public boolean isEQ()
        {
            return true;
        }

        @Override
        MultiColumnRestriction toMultiColumnRestriction()
        {
            return new MultiColumnRestriction.EQRestriction(Collections.singletonList(columnDef), term);
        }

        @Override
        public void addToRowFilter(RowFilter.Builder filter,
                                   IndexRegistry indexRegistry,
                                   QueryOptions options)
        {
            filter.add(columnDef, Operator.EQ, term.bindAndGet(options));
        }

        @Override
        public MultiCBuilder appendTo(MultiCBuilder builder, QueryOptions options)
        {
            List<MultiCBuilder.Element> element = Collections.singletonList(MultiCBuilder.Element.point(term.bindAndGet(options)));
            builder.extend(element, getColumnDefs());
            checkFalse(builder.containsNull(), "Invalid null value in condition for column %s", columnDef.name);
            checkFalse(builder.containsUnset(), "Invalid unset value for column %s", columnDef.name);
            return builder;
        }

        @Override
        public String toString()
        {
            return String.format("EQ(%s)", term);
        }

        @Override
        public SingleRestriction doMergeWith(SingleRestriction otherRestriction)
        {
            throw invalidRequest("%s cannot be restricted by more than one relation if it includes an Equal", columnDef.name);
        }

        @Override
        protected boolean isSupportedBy(Index index)
        {
            return index.supportsExpression(columnDef, Operator.EQ);
        }
    }

    public static class INRestriction extends SingleColumnRestriction
    {
        protected final MarkerOrList terms;

        public INRestriction(ColumnMetadata columnDef, MarkerOrList terms)
        {
            super(columnDef);
            this.terms = terms;
        }

        @Override
        public final boolean isIN()
        {
            return true;
        }

        @Override
        public final SingleRestriction doMergeWith(SingleRestriction otherRestriction)
        {
            throw invalidRequest("%s cannot be restricted by more than one relation if it includes a IN", columnDef.name);
        }

        @Override
        MultiColumnRestriction toMultiColumnRestriction()
        {
            throw new UnsupportedOperationException("Cannot convert to multicolumn restriction");
        }

        @Override
        public MultiCBuilder appendTo(MultiCBuilder builder, QueryOptions options)
        {
            List<ByteBuffer> values = this.terms.bindAndGet(options, columnDef.name);
            List<MultiCBuilder.Element> elements = new ArrayList<>(values.size());
            for (ByteBuffer value: values)
                elements.add(MultiCBuilder.Element.point(value));
            builder.extend(elements, getColumnDefs());
            checkFalse(builder.containsNull(), "Invalid null value in condition for column %s", columnDef.name);
            checkFalse(builder.containsUnset(), "Invalid unset value for column %s", columnDef.name);
            return builder;
        }

        @Override
        public void addToRowFilter(RowFilter.Builder filter,
                                   IndexRegistry indexRegistry,
                                   QueryOptions options)
        {
            List<ByteBuffer> values = this.terms.bindAndGet(options, columnDef.name);
            for (ByteBuffer v : values)
            {
                checkNotNull(v, "Invalid null value for column %s", columnDef.name);
                checkBindValueSet(v, "Invalid unset value for column %s", columnDef.name);
            }
            ByteBuffer buffer = ListSerializer.pack(values, values.size());
            filter.add(columnDef, Operator.IN, buffer);
        }

        @Override
        public void addFunctionsTo(List<Function> functions)
        {
            terms.addFunctionsTo(functions);
        }

        @Override
        public final boolean isSupportedBy(Index index)
        {
            return index.supportsExpression(columnDef, Operator.IN);
        }

        @Override
        public String toString()
        {
            return String.format("IN(%s)", terms);
        }
    }

    public static class SliceRestriction extends SingleColumnRestriction
    {
        private final TermSlice slice;
        private final List<MarkerOrList> skippedValues; // values passed in NOT IN

        private SliceRestriction(ColumnMetadata columnDef, TermSlice slice, List<MarkerOrList> skippedValues)
        {
            super(columnDef);
            assert slice != null;
            assert skippedValues != null;
            this.slice = slice;
            this.skippedValues = skippedValues;
        }

        public static SliceRestriction fromBound(ColumnMetadata columnDef, Bound bound, boolean inclusive, Term term)
        {
            TermSlice slice = TermSlice.newInstance(bound, inclusive, term);
            return new SliceRestriction(columnDef, slice, Collections.emptyList());
        }

        public static SliceRestriction fromSkippedValues(ColumnMetadata columnDef, MarkerOrList skippedValues)
        {
            return new SliceRestriction(columnDef, TermSlice.UNBOUNDED, Collections.singletonList(skippedValues));
        }

        @Override
        public void addFunctionsTo(List<Function> functions)
        {
            slice.addFunctionsTo(functions);
        }

        @Override
        MultiColumnRestriction toMultiColumnRestriction()
        {
            return new MultiColumnRestriction.SliceRestriction(Collections.singletonList(columnDef), slice, skippedValues);
        }

        @Override
        public boolean isSlice()
        {
            return true;
        }

        @Override
        public MultiCBuilder appendTo(MultiCBuilder builder, QueryOptions options)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean hasBound(Bound b)
        {
            return slice.hasBound(b);
        }

        @Override
        public MultiCBuilder appendBoundTo(MultiCBuilder builder, Bound bound, QueryOptions options)
        {
            Bound b = bound.reverseIfNeeded(getFirstColumn());

            List<MultiCBuilder.Element> toAdd = new ArrayList<>(skippedValues.size() + 1);
            if (hasBound(b))
            {
                ByteBuffer value = slice.bound(b).bindAndGet(options);
                checkBindValueSet(value, "Invalid unset value for column %s", columnDef.name);
                toAdd.add(MultiCBuilder.Element.bound(value, bound, slice.isInclusive(b)));
            }
            else
            {
                toAdd.add(bound.isStart() ? MultiCBuilder.Element.BOTTOM : MultiCBuilder.Element.TOP);
            }

            for (MarkerOrList markerOrList: skippedValues)
            {
                for (ByteBuffer value: markerOrList.bindAndGet(options, columnDef.name))
                {
                    checkBindValueSet(value, "Invalid unset value for column %s", columnDef.name);
                    toAdd.add(MultiCBuilder.Element.bound(value, bound, false));
                }
            }
            return builder.extend(toAdd, getColumnDefs());
        }

        @Override
        public boolean isInclusive(Bound b)
        {
            return slice.isInclusive(b);
        }

        @Override
        public SingleRestriction doMergeWith(SingleRestriction otherRestriction)
        {
            checkTrue(otherRestriction.isSlice(),
                      "Column \"%s\" cannot be restricted by both an equality and an inequality relation",
                      columnDef.name);

            SingleColumnRestriction.SliceRestriction otherSlice = (SingleColumnRestriction.SliceRestriction) otherRestriction;

            checkFalse(hasBound(Bound.START) && otherSlice.hasBound(Bound.START),
                       "More than one restriction was found for the start bound on %s", columnDef.name);

            checkFalse(hasBound(Bound.END) && otherSlice.hasBound(Bound.END),
                       "More than one restriction was found for the end bound on %s", columnDef.name);

            List<MarkerOrList> newSkippedValues = new ArrayList<>(skippedValues.size() + otherSlice.skippedValues.size());
            newSkippedValues.addAll(skippedValues);
            newSkippedValues.addAll(otherSlice.skippedValues);
            return new SliceRestriction(columnDef,  slice.merge(otherSlice.slice), newSkippedValues);
        }

        @Override
        public void addToRowFilter(RowFilter.Builder filter, IndexRegistry indexRegistry, QueryOptions options)
        {
            for (Bound b : Bound.values())
                if (hasBound(b))
                    filter.add(columnDef, slice.getIndexOperator(b), slice.bound(b).bindAndGet(options));

            for (MarkerOrList markerOrList: skippedValues)
            {
                for (ByteBuffer value : markerOrList.bindAndGet(options, columnDef.name))
                   filter.add(columnDef, Operator.NEQ, value);
            }
        }

        @Override
        protected boolean isSupportedBy(Index index)
        {
            return slice.isSupportedBy(columnDef, index);
        }

        @Override
        public String toString()
        {
            return String.format("SLICE{%s, NOT IN %s}", slice, skippedValues);
        }

    }

    // This holds CONTAINS, CONTAINS_KEY, NOT CONTAINS, NOT CONTAINS KEY and map[key] = value restrictions because we might want to have any combination of them.
    public static final class ContainsRestriction extends SingleColumnRestriction
    {
        private final List<Term> values = new ArrayList<>(); // for CONTAINS
        private final List<Term> negativeValues = new ArrayList<>(); // for NOT_CONTAINS
        private final List<Term> keys = new ArrayList<>(); // for CONTAINS_KEY
        private final List<Term> negativeKeys = new ArrayList<>(); // for NOT_CONTAINS_KEY
        private final List<Term> entryKeys = new ArrayList<>(); // for map[key] = value
        private final List<Term> entryValues = new ArrayList<>(); // for map[key] = value
        private final List<Term> negativeEntryKeys = new ArrayList<>(); // for map[key] != value
        private final List<Term> negativeEntryValues = new ArrayList<>(); // for map[key] != value

        public ContainsRestriction(ColumnMetadata columnDef, Term t, boolean isKey, boolean isNot)
        {
            super(columnDef);
            if (isNot)
            {
                if (isKey)
                    negativeKeys.add(t);
                else
                    negativeValues.add(t);
            }
            else
            {
                if (isKey)
                    keys.add(t);
                else
                    values.add(t);
            }
        }

        public ContainsRestriction(ColumnMetadata columnDef, Term mapKey, Term mapValue, boolean isNot)
        {
            super(columnDef);
            if (isNot)
            {
                negativeEntryKeys.add(mapKey);
                negativeEntryValues.add(mapValue);
            }
            else
            {
                entryKeys.add(mapKey);
                entryValues.add(mapValue);
            }
        }

        @Override
        MultiColumnRestriction toMultiColumnRestriction()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        boolean canBeConvertedToMultiColumnRestriction()
        {
            return false;
        }

        @Override
        public MultiCBuilder appendTo(MultiCBuilder builder, QueryOptions options)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isContains()
        {
            return true;
        }

        @Override
        public SingleRestriction doMergeWith(SingleRestriction otherRestriction)
        {
            checkTrue(otherRestriction.isContains(),
                      "Collection column %s can only be restricted by CONTAINS, CONTAINS KEY, or map-entry equality",
                      columnDef.name);

            SingleColumnRestriction.ContainsRestriction newContains = new ContainsRestriction(columnDef);
            copyKeysAndValues(this, newContains);
            copyKeysAndValues((ContainsRestriction) otherRestriction, newContains);

            return newContains;
        }

        @Override
        public void addToRowFilter(RowFilter.Builder filter, IndexRegistry indexRegistry, QueryOptions options)
        {
            for (ByteBuffer value : bindAndGet(values, options))
                filter.add(columnDef, Operator.CONTAINS, value);
            for (ByteBuffer key : bindAndGet(keys, options))
                filter.add(columnDef, Operator.CONTAINS_KEY, key);
            for (ByteBuffer value : bindAndGet(negativeValues, options))
                filter.add(columnDef, Operator.NOT_CONTAINS, value);
            for (ByteBuffer key : bindAndGet(negativeKeys, options))
                filter.add(columnDef, Operator.NOT_CONTAINS_KEY, key);

            List<ByteBuffer> eks = bindAndGet(entryKeys, options);
            List<ByteBuffer> evs = bindAndGet(entryValues, options);
            assert eks.size() == evs.size();
            for (int i = 0; i < eks.size(); i++)
                filter.addMapEquality(columnDef, eks.get(i), Operator.EQ, evs.get(i));

            List<ByteBuffer> neks = bindAndGet(negativeEntryKeys, options);
            List<ByteBuffer> nevs = bindAndGet(negativeEntryValues, options);
            assert neks.size() == nevs.size();
            for (int i = 0; i < neks.size(); i++)
                filter.addMapEquality(columnDef, neks.get(i), Operator.NEQ, nevs.get(i));

        }

        @Override
        protected boolean isSupportedBy(Index index)
        {
            boolean supported = false;

            if (numberOfValues() > 0)
                supported |= index.supportsExpression(columnDef, Operator.CONTAINS);

            if (numberOfKeys() > 0)
                supported |= index.supportsExpression(columnDef, Operator.CONTAINS_KEY);

            if (numberOfNegativeValues() > 0)
                supported |= index.supportsExpression(columnDef, Operator.NOT_CONTAINS);

            if (numberOfNegativeKeys() > 0)
                supported |= index.supportsExpression(columnDef, Operator.NOT_CONTAINS_KEY);

            if (numberOfEntries() > 0)
                supported |= index.supportsExpression(columnDef, Operator.EQ);

            if (numberOfNegativeEntries() > 0)
                supported |= index.supportsExpression(columnDef, Operator.NEQ);

            return supported;
        }

        @Override
        public boolean needsFiltering(Index.Group indexGroup)
        {
            // multiple contains might require filtering on some indexes, since that is equivalent to a disjunction (or)
            boolean hasMultipleContains = (numberOfValues() + numberOfKeys() + numberOfEntries()) > 1;

            for (Index index : indexGroup.getIndexes())
            {
                if (isSupportedBy(index) && !(hasMultipleContains && index.filtersMultipleContains()))
                    return false;
            }

            return true;
        }

        public int numberOfValues()
        {
            return values.size();
        }

        public int numberOfNegativeValues()
        {
            return negativeValues.size();
        }

        public int numberOfKeys()
        {
            return keys.size();
        }

        public int numberOfNegativeKeys()
        {
            return negativeKeys.size();
        }

        public int numberOfEntries()
        {
            return entryKeys.size();
        }

        public int numberOfNegativeEntries()
        {
            return negativeEntryKeys.size();
        }

        @Override
        public void addFunctionsTo(List<Function> functions)
        {
            Terms.addFunctions(values, functions);
            Terms.addFunctions(keys, functions);
            Terms.addFunctions(entryKeys, functions);
            Terms.addFunctions(entryValues, functions);

            Terms.addFunctions(negativeValues, functions);
            Terms.addFunctions(negativeKeys, functions);
            Terms.addFunctions(negativeEntryKeys, functions);
            Terms.addFunctions(negativeEntryValues, functions);
        }

        @Override
        public String toString()
        {
            return String.format("CONTAINS(values=%s, keys=%s, entryKeys=%s, entryValues=%s)",
                                 values, keys, entryKeys, entryValues);
        }

        @Override
        public boolean hasBound(Bound b)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public MultiCBuilder appendBoundTo(MultiCBuilder builder, Bound bound, QueryOptions options)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isInclusive(Bound b)
        {
            throw new UnsupportedOperationException();
        }

        /**
         * Binds the query options to the specified terms and returns the resulting values.
         *
         * @param terms the terms
         * @param options the query options
         * @return the value resulting from binding the query options to the specified terms
         */
        private static List<ByteBuffer> bindAndGet(List<Term> terms, QueryOptions options)
        {
            List<ByteBuffer> buffers = new ArrayList<>(terms.size());
            for (Term value : terms)
                buffers.add(value.bindAndGet(options));
            return buffers;
        }

        /**
         * Copies the keys and value from the first <code>Contains</code> to the second one.
         *
         * @param from the <code>Contains</code> to copy from
         * @param to the <code>Contains</code> to copy to
         */
        private static void copyKeysAndValues(ContainsRestriction from, ContainsRestriction to)
        {
            to.values.addAll(from.values);
            to.negativeValues.addAll(from.negativeValues);
            to.keys.addAll(from.keys);
            to.negativeKeys.addAll(from.negativeKeys);
            to.entryKeys.addAll(from.entryKeys);
            to.entryValues.addAll(from.entryValues);
            to.negativeEntryKeys.addAll(from.negativeEntryKeys);
            to.negativeEntryValues.addAll(from.negativeEntryValues);

        }

        private ContainsRestriction(ColumnMetadata columnDef)
        {
            super(columnDef);
        }
    }


    public static final class IsNotNullRestriction extends SingleColumnRestriction
    {
        public IsNotNullRestriction(ColumnMetadata columnDef)
        {
            super(columnDef);
        }

        @Override
        public void addFunctionsTo(List<Function> functions)
        {
        }

        @Override
        public boolean isNotNull()
        {
            return true;
        }

        @Override
        MultiColumnRestriction toMultiColumnRestriction()
        {
            return new MultiColumnRestriction.NotNullRestriction(Collections.singletonList(columnDef));
        }

        @Override
        public void addToRowFilter(RowFilter.Builder filter,
                                   IndexRegistry indexRegistry,
                                   QueryOptions options)
        {
            throw new UnsupportedOperationException("Secondary indexes do not support IS NOT NULL restrictions");
        }

        @Override
        public MultiCBuilder appendTo(MultiCBuilder builder, QueryOptions options)
        {
            throw new UnsupportedOperationException("Cannot use IS NOT NULL restriction for slicing");
        }

        @Override
        public String toString()
        {
            return "IS NOT NULL";
        }

        @Override
        public SingleRestriction doMergeWith(SingleRestriction otherRestriction)
        {
            throw invalidRequest("%s cannot be restricted by a relation if it includes an IS NOT NULL", columnDef.name);
        }

        @Override
        protected boolean isSupportedBy(Index index)
        {
            return index.supportsExpression(columnDef, Operator.IS_NOT);
        }
    }

    public static final class LikeRestriction extends SingleColumnRestriction
    {
        private static final ByteBuffer LIKE_WILDCARD = ByteBufferUtil.bytes("%");
        private final Operator operator;
        private final Term value;

        public LikeRestriction(ColumnMetadata columnDef, Operator operator, Term value)
        {
            super(columnDef);
            this.operator = operator;
            this.value = value;
        }

        @Override
        public void addFunctionsTo(List<Function> functions)
        {
            value.addFunctionsTo(functions);
        }

        @Override
        public boolean isEQ()
        {
            return false;
        }

        @Override
        public boolean isLIKE()
        {
            return true;
        }

        @Override
        public boolean canBeConvertedToMultiColumnRestriction()
        {
            return false;
        }

        @Override
        MultiColumnRestriction toMultiColumnRestriction()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void addToRowFilter(RowFilter.Builder filter,
                                   IndexRegistry indexRegistry,
                                   QueryOptions options)
        {
            Pair<Operator, ByteBuffer> operation = makeSpecific(value.bindAndGet(options));

            // there must be a suitable INDEX for LIKE_XXX expressions
            RowFilter.SimpleExpression expression = filter.add(columnDef, operation.left, operation.right);
            indexRegistry.getBestIndexFor(expression)
                         .orElseThrow(() -> invalidRequest("%s is only supported on properly indexed columns",
                                                           expression));
        }

        @Override
        public MultiCBuilder appendTo(MultiCBuilder builder, QueryOptions options)
        {
            // LIKE can be used with clustering columns, but as it doesn't
            // represent an actual clustering value, it can't be used in a
            // clustering filter.
            throw new UnsupportedOperationException();
        }

        @Override
        public String toString()
        {
            return operator.toString();
        }

        @Override
        public SingleRestriction doMergeWith(SingleRestriction otherRestriction)
        {
            throw invalidRequest("%s cannot be restricted by more than one relation if it includes a %s", columnDef.name, operator);
        }

        @Override
        protected boolean isSupportedBy(Index index)
        {
            return index.supportsExpression(columnDef, operator);
        }

        /**
         * As the specific subtype of LIKE (LIKE_PREFIX, LIKE_SUFFIX, LIKE_CONTAINS, LIKE_MATCHES) can only be
         * determined by examining the value, which in turn can only be known after binding, all LIKE restrictions
         * are initially created with the generic LIKE operator. This function takes the bound value, trims the
         * wildcard '%' chars from it and returns a tuple of the inferred operator subtype and the final value
         * @param value the bound value for the LIKE operation
         * @return  Pair containing the inferred LIKE subtype and the value with wildcards removed
         */
        private static Pair<Operator, ByteBuffer> makeSpecific(ByteBuffer value)
        {
            Operator operator;
            int beginIndex = value.position();
            int endIndex = value.limit() - 1;
            if (ByteBufferUtil.endsWith(value, LIKE_WILDCARD))
            {
                if (ByteBufferUtil.startsWith(value, LIKE_WILDCARD))
                {
                    operator = Operator.LIKE_CONTAINS;
                    beginIndex =+ 1;
                }
                else
                {
                    operator = Operator.LIKE_PREFIX;
                }
            }
            else if (ByteBufferUtil.startsWith(value, LIKE_WILDCARD))
            {
                operator = Operator.LIKE_SUFFIX;
                beginIndex += 1;
                endIndex += 1;
            }
            else
            {
                operator = Operator.LIKE_MATCHES;
                endIndex += 1;
            }

            if (endIndex == 0 || beginIndex == endIndex)
                throw invalidRequest("LIKE value can't be empty.");

            ByteBuffer newValue = value.duplicate();
            newValue.position(beginIndex);
            newValue.limit(endIndex);
            return Pair.create(operator, newValue);
        }
    }

    public static final class AnnRestriction extends SingleColumnRestriction
    {
        private final Term value;

        public AnnRestriction(ColumnMetadata columnDef, Term value)
        {
            super(columnDef);
            this.value = value;
        }

        public ByteBuffer value(QueryOptions options)
        {
            return value.bindAndGet(options);
        }

        @Override
        public void addFunctionsTo(List<Function> functions)
        {
            value.addFunctionsTo(functions);
        }

        @Override
        MultiColumnRestriction toMultiColumnRestriction()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void addToRowFilter(RowFilter.Builder filter,
                                   IndexRegistry indexRegistry,
                                   QueryOptions options)
        {
            filter.add(columnDef, Operator.ANN, value.bindAndGet(options));
        }

        @Override
        public MultiCBuilder appendTo(MultiCBuilder builder, QueryOptions options)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public String toString()
        {
            return String.format("ANN(%s)", value);
        }

        @Override
        public SingleRestriction doMergeWith(SingleRestriction otherRestriction)
        {
            // VSTODO not sure if this is right
            throw new UnsupportedOperationException();
        }

        @Override
        protected boolean isSupportedBy(Index index)
        {
            return index.supportsExpression(columnDef, Operator.ANN);
        }

        @Override
        public boolean isAnn()
        {
            return true;
        }
    }

    public static final class AnalyzerMatchesRestriction extends SingleColumnRestriction
    {
        private final List<Term> values;

        public AnalyzerMatchesRestriction(ColumnMetadata columnDef, Term value)
        {
            super(columnDef);
            this.values = Collections.singletonList(value);
        }

        public AnalyzerMatchesRestriction(ColumnMetadata columnDef, List<Term> values)
        {
            super(columnDef);
            this.values = values;
        }

        List<Term> getValues()
        {
            return values;
        }

        @Override
        public void addFunctionsTo(List<Function> functions)
        {
            for (Term value : values)
            {
                value.addFunctionsTo(functions);
            }
        }

        @Override
        MultiColumnRestriction toMultiColumnRestriction()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void addToRowFilter(RowFilter.Builder filter,
                                   IndexRegistry indexRegistry,
                                   QueryOptions options)
        {
            for (Term value : values)
            {
                filter.add(columnDef, Operator.ANALYZER_MATCHES, value.bindAndGet(options));
            }
        }

        @Override
        public MultiCBuilder appendTo(MultiCBuilder builder, QueryOptions options)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public String toString()
        {
            return String.format("ANALYZER_MATCHES(%s)", values);
        }

        /**
         * Merges this restriction with another restriction. Only called for conjuctive restrictions.
         */
        @Override
        public SingleRestriction doMergeWith(SingleRestriction otherRestriction)
        {
            if (!(otherRestriction instanceof AnalyzerMatchesRestriction))
                throw new UnsupportedOperationException();
            List<Term> otherValues = ((AnalyzerMatchesRestriction) otherRestriction).getValues();
            List<Term> newValues = new ArrayList<>(values.size() + otherValues.size());
            newValues.addAll(values);
            newValues.addAll(otherValues);
            return new AnalyzerMatchesRestriction(columnDef, newValues);
        }


        @Override
        protected boolean isSupportedBy(Index index)
        {
            return index.supportsExpression(columnDef, Operator.ANALYZER_MATCHES);
        }
    }
}

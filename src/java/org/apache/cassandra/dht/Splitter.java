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

package org.apache.cassandra.dht;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;

import static java.util.stream.Collectors.toSet;

/**
 * Partition splitter.
 */
public abstract class Splitter
{
    private final IPartitioner partitioner;

    protected Splitter(IPartitioner partitioner)
    {
        this.partitioner = partitioner;
    }

    @VisibleForTesting
    protected abstract Token tokenForValue(BigInteger value);

    @VisibleForTesting
    protected abstract BigInteger valueForToken(Token token);

    @VisibleForTesting
    protected BigInteger tokensInRange(Range<Token> range)
    {
        //full range case
        if (range.left.equals(range.right))
            return tokensInRange(new Range(partitioner.getMinimumToken(), partitioner.getMaximumToken()));

        BigInteger totalTokens = BigInteger.ZERO;
        for (Range<Token> unwrapped : range.unwrap())
        {
            totalTokens = totalTokens.add(valueForToken(token(unwrapped.right)).subtract(valueForToken(unwrapped.left))).abs();
        }
        return totalTokens;
    }

    /**
     * Computes the number of elapsed tokens from the range start until this token
     * @return the number of tokens from the range start to the token
     */
    @VisibleForTesting
    protected BigInteger elapsedTokens(Token token, Range<Token> range)
    {
        // No token elapsed since range does not contain token
        if (!range.contains(token))
            return BigInteger.ZERO;

        BigInteger elapsedTokens = BigInteger.ZERO;
        for (Range<Token> unwrapped : range.unwrap())
        {
            if (unwrapped.contains(token))
            {
                elapsedTokens = elapsedTokens.add(tokensInRange(new Range<>(unwrapped.left, token)));
            }
            else if (token.compareTo(unwrapped.left) < 0)
            {
                elapsedTokens = elapsedTokens.add(tokensInRange(unwrapped));
            }
        }
        return elapsedTokens;
    }

    /**
     * Computes the normalized position of this token relative to this range
     * @return A number between 0.0 and 1.0 representing this token's position
     * in this range or -1.0 if this range doesn't contain this token.
     */
    public double positionInRange(Token token, Range<Token> range)
    {
        //full range case
        if (range.left.equals(range.right))
            return positionInRange(token, new Range(partitioner.getMinimumToken(), partitioner.getMaximumToken()));

        // leftmost token means we are on position 0.0
        if (token.equals(range.left))
            return 0.0;

        // rightmost token means we are on position 1.0
        if (token.equals(range.right))
            return 1.0;

        // Impossible to find position when token is not contained in range
        if (!range.contains(token))
            return -1.0;

        return new BigDecimal(elapsedTokens(token, range)).divide(new BigDecimal(tokensInRange(range)), 3, BigDecimal.ROUND_HALF_EVEN).doubleValue();
    }

    /**
     * How local ranges should be split
     */
    public enum SplitType
    {
        /** Local ranges should always be split, without attempting to keep them whole */
        ALWAYS_SPLIT,
        /** A first pass will try to avoid splitting ranges, but if there aren't enough parts,
         * then ranges will be split in a second pass.
         */
        PREFER_WHOLE,
        /** Ranges Should never be split */
        ONLY_WHOLE
    }

    /**
     * The result of a split operation, this is just a wrapper of the boundaries and the type
     * of split that was done, i.e. if the local ranges were split or not. This is just so that
     * we can test the algorithm.
     */
    public final static class SplitResult
    {
        public final List<Token> boundaries;
        public final boolean rangesWereSplit;

        SplitResult(List<Token> boundaries, boolean rangesWereSplit)
        {
            this.boundaries = boundaries;
            this.rangesWereSplit = rangesWereSplit;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o)
                return true;

            if (!(o instanceof SplitResult))
                return false;

            SplitResult splitResult = (SplitResult) o;
            return Objects.equals(boundaries, splitResult.boundaries)
                   && Objects.equals(rangesWereSplit, splitResult.rangesWereSplit);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(boundaries, rangesWereSplit);
        }
    }

    /**
     * Split the local ranges into the specified number of parts.
     *
     * Depending on the parameter {@code splitType}, it may attempt to only merge the local ranges first,
     * to see if this is sufficient to cover the requested number of parts. If it's not, it will then split
     * existing ranges.
     *
     * @param parts the number of parts
     * @param weightedRanges the local ranges owned by this node
     * @param splitType how local ranges should be split, see {@link SplitType}
     *
     * @return the split result, which contains a list of tokens, one per part, and if the ranges were split or not
     */
    public SplitResult splitOwnedRanges(int parts, List<WeightedRange> weightedRanges, SplitType splitType)
    {
        if (weightedRanges.isEmpty() || parts <= 1)
            return new SplitResult(Collections.singletonList(partitioner.getMaximumToken()), false);

        BigInteger totalTokens = BigInteger.ZERO;
        for (WeightedRange weightedRange : weightedRanges)
        {
            totalTokens = totalTokens.add(weightedRange.totalTokens(this));
        }

        BigInteger perPart = totalTokens.divide(BigInteger.valueOf(parts));
        // the range owned is so tiny we can't split it:
        if (perPart.equals(BigInteger.ZERO))
            return new SplitResult(Collections.singletonList(partitioner.getMaximumToken()), false);

        List<Token> boundaries = new ArrayList<>();

        if (splitType != SplitType.ALWAYS_SPLIT)
        {
            // see if we can obtain a sufficient number of parts by only merging local ranges
            boundaries = splitOwnedRangesNoPartialRanges(weightedRanges, perPart, parts);
            // we were either able to obtain sufficient parts without splitting ranges or we should never split ranges
            if (splitType == SplitType.ONLY_WHOLE || boundaries.size() == parts)
                return new SplitResult(boundaries, false);
            else
                boundaries.clear();
        }

        // otherwise continue by splitting ranges

        BigInteger sum = BigInteger.ZERO;
        BigInteger tokensLeft = totalTokens;
        for (WeightedRange weightedRange : weightedRanges)
        {
            BigInteger currentRangeWidth = weightedRange.totalTokens(this);
            BigInteger left = valueForToken(weightedRange.left());
            BigInteger currentRangeFactor = BigInteger.valueOf(Math.max(1, (long) (1 / weightedRange.weight)));
            while (sum.add(currentRangeWidth).compareTo(perPart) >= 0)
            {
                BigInteger withinRangeBoundary = perPart.subtract(sum);
                left = left.add(withinRangeBoundary.multiply(currentRangeFactor));
                boundaries.add(tokenForValue(left));
                tokensLeft = tokensLeft.subtract(perPart);
                currentRangeWidth = currentRangeWidth.subtract(withinRangeBoundary);
                sum = BigInteger.ZERO;
                int partsLeft = parts - boundaries.size();
                if (partsLeft == 0)
                {
                    break;
                }
                else if (partsLeft == 1)
                {
                    perPart = tokensLeft;
                }
            }
            sum = sum.add(currentRangeWidth);
        }
        boundaries.set(boundaries.size() - 1, partitioner.getMaximumToken());

        assert boundaries.size() == parts : boundaries.size() + "!=" + parts + " " + boundaries + ":" + weightedRanges;
        return new SplitResult(boundaries, true);
    }

    private List<Token> splitOwnedRangesNoPartialRanges(List<WeightedRange> weightedRanges, BigInteger perPart, int parts)
    {
        List<Token> boundaries = new ArrayList<>(parts);
        BigInteger sum = BigInteger.ZERO;

        int i = 0;
        final int rangesCount = weightedRanges.size();
        while (boundaries.size() < parts - 1 && i < rangesCount - 1)
        {
            WeightedRange r = weightedRanges.get(i);
            WeightedRange nextRange = weightedRanges.get(i + 1);

            BigInteger currentRangeWidth = r.totalTokens(this);
            BigInteger nextRangeWidth = nextRange.totalTokens(this);
            sum = sum.add(currentRangeWidth);

            // does this or next range take us beyond the per part limit?
            if (sum.compareTo(perPart) > 0 || sum.add(nextRangeWidth).compareTo(perPart) > 0)
            {
                // Either this or the next range will take us beyond the perPart limit. Will stopping now or
                // adding the next range create the smallest difference to perPart?
                BigInteger diffCurrent = sum.subtract(perPart).abs();
                BigInteger diffNext = sum.add(nextRangeWidth).subtract(perPart).abs();
                if (diffNext.compareTo(diffCurrent) >= 0)
                {
                    sum = BigInteger.ZERO;
                    boundaries.add(token(r.right()));
                }
            }
            i++;
        }
        boundaries.add(partitioner.getMaximumToken());
        return boundaries;
    }

    /**
     * We avoid calculating for wrap around ranges, instead we use the actual max token, and then, when translating
     * to PartitionPositions, we include tokens from .minKeyBound to .maxKeyBound to make sure we include all tokens.
     */
    private Token token(Token t)
    {
        return t.equals(partitioner.getMinimumToken()) ? partitioner.getMaximumToken() : t;
    }

    /**
     * Splits the specified token ranges in at least {@code parts} subranges.
     * <p>
     * Each returned subrange will be contained in exactly one of the specified ranges.
     *
     * @param ranges a collection of token ranges to be split
     * @param parts the minimum number of returned ranges
     * @return at least {@code minParts} token ranges covering {@code ranges}
     */
    public Set<Range<Token>> split(Collection<Range<Token>> ranges, int parts)
    {
        int numRanges = ranges.size();
        if (numRanges >= parts)
        {
            return Sets.newHashSet(ranges);
        }
        else
        {
            int partsPerRange = (int) Math.ceil((double) parts / numRanges);
            return ranges.stream()
                         .map(range -> split(range, partsPerRange))
                         .flatMap(Collection::stream)
                         .collect(toSet());
        }
    }

    /**
     * Splits the specified token range in {@code parts} subranges, unless the range has not enough tokens in which case
     * the range will be returned without splitting.
     *
     * @param range a token range
     * @param parts the number of subranges
     * @return {@code parts} even subranges of {@code range}, or {@code range} if it is too small to be splitted
     */
    private Set<Range<Token>> split(Range<Token> range, int parts)
    {
        Token left = range.left;
        Set<Range<Token>> subranges = new LinkedHashSet<>(parts);

        for (double i = 1; i < parts; i++)
        {
            Token right = partitioner.split(range.left, range.right, i / parts);
            if (!left.equals(right))
                subranges.add(new Range<>(left, right));
            left = right;
        }
        subranges.add(new Range<>(left, range.right));
        return subranges;
    }

    public static class WeightedRange
    {
        private final double weight;
        private final Range<Token> range;

        public WeightedRange(double weight, Range<Token> range)
        {
            this.weight = weight;
            this.range = range;
        }

        public BigInteger totalTokens(Splitter splitter)
        {
            BigInteger right = splitter.valueForToken(splitter.token(range.right));
            BigInteger left = splitter.valueForToken(range.left);
            BigInteger factor = BigInteger.valueOf(Math.max(1, (long) (1 / weight)));
            BigInteger size = right.subtract(left);
            return size.abs().divide(factor);
        }

        /**
         * A less precise version of the above, returning the size of the span as a double approximation.
         */
        public double size()
        {
            return left().size(right()) * weight;
        }

        public Token left()
        {
            return range.left;
        }

        public Token right()
        {
            return range.right;
        }

        public Range<Token> range()
        {
            return range;
        }

        public double weight()
        {
            return weight;
        }

        public String toString()
        {
            return "WeightedRange{" +
                   "weight=" + weight +
                   ", range=" + range +
                   '}';
        }

        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (!(o instanceof WeightedRange)) return false;
            WeightedRange that = (WeightedRange) o;
            return Objects.equals(range, that.range);
        }

        public int hashCode()
        {
            return Objects.hash(weight, range);
        }
    }
}

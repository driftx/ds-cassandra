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

package org.apache.cassandra.index.sai.disk.vector;

import java.util.NoSuchElementException;
import java.util.PriorityQueue;

import org.junit.Test;

import io.github.jbellis.jvector.graph.similarity.ScoreFunction;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class BruteForceRowIdIteratorTest
{
    private static final VectorTypeSupport vts = VectorizationProvider.getInstance().getVectorTypeSupport();

    @Test
    public void testBruteForceRowIdIteratorForEmptyPQAndTopKEqualsLimit()
    {
        var queryVector = vts.createFloatVector(new float[] { 1f, 0f });
        var pq = new PriorityQueue<BruteForceRowIdIterator.RowWithApproximateScore>(10);
        var topK = 10;
        var limit = 10;

        // Should work for an empty pq
        var view = new TestVectorSupplier();
        JVectorLuceneOnDiskGraph.CloseableReranker reranker = new JVectorLuceneOnDiskGraph.CloseableReranker(VectorSimilarityFunction.COSINE, queryVector, view);
        var iter = new BruteForceRowIdIterator(pq, reranker, limit, topK);
        assertFalse(iter.hasNext());
        assertThrows(NoSuchElementException.class, iter::next);
        assertFalse(view.isClosed);
        iter.close();
        assertTrue(view.isClosed);
    }

    private static class TestVectorSupplier implements JVectorLuceneOnDiskGraph.VectorSupplier
    {
        private boolean isClosed = false;

        @Override
        public ScoreFunction.ExactScoreFunction getScoreFunction(VectorFloat<?> queryVector, VectorSimilarityFunction similarityFunction)
        {
            return ordinal -> {
                return similarityFunction.compare(queryVector, vts.createFloatVector(new float[] { ordinal }));
            };
        }

        @Override
        public void close()
        {
            isClosed = true;
        }
    }
}

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

package org.apache.cassandra.index.sai.utils;

import java.util.PriorityQueue;

import org.apache.cassandra.utils.AbstractIterator;

/**
 * An iterator that returns elements from a priority queue in order. This is different from
 * {@link PriorityQueue#iterator()} which returns elements in undefined order.
 */
public class PriorityQueueIterator<T> extends AbstractIterator<T>
{
    private final PriorityQueue<T> pq;

    public PriorityQueueIterator(PriorityQueue<T> pq)
    {
        this.pq = pq;
    }

    @Override
    protected T computeNext()
    {
        return !pq.isEmpty() ? pq.poll() : endOfData();
    }
}

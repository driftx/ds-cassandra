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

package org.apache.cassandra.utils;

import java.util.Collection;

import com.google.common.collect.ImmutableList;

public class Collections3
{
    public static <T> ImmutableList<T> withAppended(Iterable<T> list, T... elements)
    {
        if (elements.length == 0)
            return ImmutableList.copyOf(list);

        ImmutableList.Builder<T> builder = list instanceof Collection
                                           ? ImmutableList.builderWithExpectedSize(((Collection<T>) list).size() + elements.length)
                                           : ImmutableList.builder();
        builder.addAll(list);
        for (T element : elements)
            builder.add(element);
        return builder.build();
    }
}

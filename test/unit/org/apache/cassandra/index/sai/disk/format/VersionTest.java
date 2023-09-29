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
package org.apache.cassandra.index.sai.disk.format;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class VersionTest
{
    @BeforeClass
    public static void initialise() throws Throwable
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void testVersionsSorted()
    {
        Version previous = null;
        for (Version version : Version.ALL)
        {
            if (previous != null)
                assertTrue(previous.onOrAfter(version));
            previous = version;
        }
    }

    @Test
    public void supportedVersionsWillParse()
    {
        assertEquals(Version.AA, Version.parse("aa"));
        assertEquals(Version.BA, Version.parse("ba"));
        assertEquals(Version.CA, Version.parse("ca"));
    }

    @Test
    public void unsupportedOrInvalidVersionsDoNotParse()
    {
        assertThatThrownBy(() -> Version.parse(null)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> Version.parse("ab")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> Version.parse("a")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> Version.parse("abc")).isInstanceOf(IllegalArgumentException.class);
    }
}

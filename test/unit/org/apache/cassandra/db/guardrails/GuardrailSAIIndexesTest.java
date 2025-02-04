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

package org.apache.cassandra.db.guardrails;

import java.util.stream.StreamSupport;

import com.google.common.base.Strings;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.index.sai.StorageAttachedIndex;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;

public class GuardrailSAIIndexesTest extends GuardrailTester
{
    private static int totalExistingIndexes;
    private int defaultSAIPerTableFailureThreshold;
    private int defaultSAITotalFailureThreshold;

    @BeforeClass
    public static void setup()
    {
        // Some existing system tables may have indexes (e.g. Paxos.PaxosUncommittedIndex)
        totalExistingIndexes = totalIndexes();
    }
    
    @Before
    public void before()
    {
        defaultSAIPerTableFailureThreshold = DatabaseDescriptor.getGuardrailsConfig().getStorageAttachedIndexesPerTableFailThreshold();
        defaultSAITotalFailureThreshold = DatabaseDescriptor.getGuardrailsConfig().getStorageAttachedIndexesTotalFailThreshold();
        DatabaseDescriptor.getGuardrailsConfig().setStorageAttachedIndexesPerTableThreshold(-1, 1);
        DatabaseDescriptor.getGuardrailsConfig().setStorageAttachedIndexesTotalThreshold(-1, 2);
    }

    @After
    public void after()
    {
        DatabaseDescriptor.getGuardrailsConfig().setStorageAttachedIndexesPerTableThreshold(-1, defaultSAIPerTableFailureThreshold);
        DatabaseDescriptor.getGuardrailsConfig().setStorageAttachedIndexesTotalThreshold(-1, defaultSAITotalFailureThreshold);
    }

//    @Test
//    public void testDefaultsOnPrem()
//    {
//        testDefaults(false);
//    }
//
//    @Test
//    public void testDefaultsDBAAS()
//    {
//        testDefaults(true);
//    }
//
//    public void testDefaults(boolean dbaas)
//    {
//        boolean previous = DatabaseDescriptor.isEmulateDbaasDefaults();
//        try
//        {
//            DatabaseDescriptor.setEmulateDbaasDefaults(dbaas);
//
//            GuardrailsConfig config = new GuardrailsConfig();
//            config.applyConfig();
//
//            assertEquals(GuardrailsConfig.DEFAULT_INDEXES_PER_TABLE_THRESHOLD, (int) config.sai_indexes_per_table_failure_threshold);
//            assertEquals(GuardrailsConfig.DEFAULT_INDEXES_TOTAL_THRESHOLD, (int) config.sai_indexes_total_failure_threshold);
//        }
//        finally
//        {
//            DatabaseDescriptor.setEmulateDbaasDefaults(previous);
//        }
//    }

    @Test
    public void testPerTableFailureThreshold() throws Throwable
    {
        createTable("CREATE TABLE %s (k int primary key, v1 int, v2 int, v3 int)");
        String indexName = createIndex(getCreateIndexStatement("v1"));
        assertIndexesOnCurrentTable(1);

        assertIndexCreationFails("", "v2");
        assertIndexCreationFails("custom_index_name", "v2");
        assertIndexesOnCurrentTable(1);

        // guardrail should not affect indexes of other types
        assertValid(getDifferentCreateIndexStatement("idx2", "v2"));
        assertIndexesOnCurrentTable(2);

        // drop the first index, we should be able to create new index again
        dropIndex(format("DROP INDEX %s.%s", keyspace(), indexName));
        assertIndexesOnCurrentTable(1);

        execute(getCreateIndexStatement("v3"));
        assertIndexesOnCurrentTable(2);

        // previous guardrail should not apply to another base table
        createTable("CREATE TABLE %s (k int primary key, v1 int, v2 int)");
        assertValid(getCreateIndexStatement("v1"));
        assertIndexesOnCurrentTable(1);

        assertIndexCreationFails("custom_index_name2", "v2");
        assertIndexesOnCurrentTable(1);
    }

    @Test
    public void testTotalFailureThreshold() throws Throwable
    {
        createTable("CREATE TABLE %s (k int primary key, v1 int, v2 int)");
        String indexName = createIndex(getCreateIndexStatement("v1"));
        assertTotalIndexesOfTheSameType(1);
        assertGlobalIndexes(1);

        // Create index on new table
        createTable("CREATE TABLE %s (k int primary key, v1 int, v2 int)");
        createIndex(getCreateIndexStatement("v1"));
        assertTotalIndexesOfTheSameType(2);
        assertGlobalIndexes(2);

        // Trying create new indexes on current table should fail
        assertIndexCreationFails("", "v2");
        assertIndexCreationFails("custom_index_name", "v2");
        assertTotalIndexesOfTheSameType(2);
        assertGlobalIndexes(2);

        // Trying to create indexes on new table should also fail
        createTable("CREATE TABLE %s (k int primary key, v1 int, v2 int)");
        assertIndexCreationFails("", "v1");

        // Trying to create different index type should not fail
        assertValid(getDifferentCreateIndexStatement("idx2", "v2"));
        assertTotalIndexesOfTheSameType(2);
        assertGlobalIndexes(3);

        // drop the first index, we should be able to create new index again
        dropIndex(format("DROP INDEX %s.%s", keyspace(), indexName));
        assertTotalIndexesOfTheSameType(1);
        assertGlobalIndexes(2);

        // Now index creation should succeed
        createIndex(getCreateIndexStatement("v1"));
        assertTotalIndexesOfTheSameType(2);
        assertGlobalIndexes(3);
    }

    @Test
    public void testExcludedUsers() throws Throwable
    {
        createTable("CREATE TABLE %s (k int primary key, v1 int, v2 int)");
        testExcludedUsers(() -> getCreateIndexStatement("excluded_1", "v1"),
                          () -> getCreateIndexStatement("excluded_2", "v2"),
                          () -> "DROP INDEX excluded_1",
                          () -> "DROP INDEX excluded_2");
    }

    private void assertIndexesOnCurrentTable(int count)
    {
        assertEquals(count, getCurrentColumnFamilyStore().indexManager.listIndexes().size());
    }

    private void assertGlobalIndexes(int count)
    {
        assertEquals(totalExistingIndexes + count, totalIndexes());
    }

    private static int totalIndexes()
    {
        return StreamSupport.stream(Keyspace.all().spliterator(), false).flatMap(k -> k.getColumnFamilyStores().stream()).mapToInt(t -> t.indexManager.listIndexes().size()).sum();
    }

    private void assertTotalIndexesOfTheSameType(int count)
    {
        int totalIndexes = (int) StreamSupport.stream(Keyspace.all().spliterator(), false).flatMap(k -> k.getColumnFamilyStores().stream())
                                              .flatMap(t -> t.indexManager.listIndexes().stream())
                                              .filter(i -> i.getIndexMetadata().getIndexClassName().equals(getIndexClassName())).count();
        assertEquals(count, totalIndexes);
    }

    private void assertIndexCreationFails(String indexName, String column) throws Throwable
    {
        String expectedMessage = String.format("aborting the creation of secondary index %son table %s",
                                               Strings.isNullOrEmpty(indexName) ? "" : indexName + " ", currentTable());
        assertFails(getCreateIndexStatement(indexName, column), expectedMessage);
    }

    protected String getIndexClassName()
    {
        return StorageAttachedIndex.class.getName();
    }

    String getCreateIndexStatement(String column)
    {
        return String.format("CREATE CUSTOM INDEX ON %%s (%s) USING '%s'", column, StorageAttachedIndex.class.getCanonicalName());
    }

    String getCreateIndexStatement(String indexName, String column)
    {
        return String.format("CREATE CUSTOM INDEX %s ON %%s (%s) USING '%s'", indexName, column, StorageAttachedIndex.class.getCanonicalName());
    }

    String getDifferentCreateIndexStatement(String indexName, String column)
    {
        return String.format("CREATE INDEX %s ON %%s (%s)", indexName, column);
    }
}
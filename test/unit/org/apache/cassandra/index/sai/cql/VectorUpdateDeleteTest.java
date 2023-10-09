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

package org.apache.cassandra.index.sai.cql;

import org.junit.Test;

import org.apache.cassandra.cql3.UntypedResultSet;

import static org.apache.cassandra.index.sai.cql.VectorTypeTest.assertContainsInt;
import static org.assertj.core.api.Assertions.assertThat;

public class VectorUpdateDeleteTest extends VectorTester
{

    // partition delete won't trigger UpdateTransaction#onUpdated
    @Test
    public void partitionDeleteVectorInMemoryTest() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, str_val text, val vector<float, 3>, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");
        waitForTableIndexesQueryable();

        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [1.0, 2.0, 3.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (1, 'B', [2.0, 3.0, 4.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (2, 'C', [3.0, 4.0, 5.0])");

        UntypedResultSet result = execute("SELECT * FROM %s ORDER BY val ann of [2.5, 3.5, 4.5] LIMIT 3");
        assertThat(result).hasSize(3);

        execute("UPDATE %s SET val = null WHERE pk = 0");

        result = execute("SELECT * FROM %s ORDER BY val ann of [1.1, 2.1, 3.1] LIMIT 1"); // closer to row 0
        assertThat(result).hasSize(1);
        assertContainsInt(result, "pk", 1);

        execute("DELETE from %s WHERE pk = 1");
        result = execute("SELECT * FROM %s ORDER BY val ann of [2.1, 3.1, 4.1] LIMIT 1"); // closer to row 1
        assertThat(result).hasSize(1);
        assertContainsInt(result, "pk", 2);

        flush();

        result = execute("SELECT * FROM %s ORDER BY val ann of [2.1, 3.1, 4.1] LIMIT 1");  // closer to row 1
        assertThat(result).hasSize(1);
        assertContainsInt(result, "pk", 2);
    }

    // row delete will trigger UpdateTransaction#onUpdated
    @Test
    public void rowDeleteVectorInMemoryAndFlushTest() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, str_val text, val vector<float, 3>, PRIMARY KEY(pk, ck))");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");
        waitForTableIndexesQueryable();

        execute("INSERT INTO %s (pk, ck, str_val, val) VALUES (0, 0, 'A', [1.0, 2.0, 3.0])");
        execute("INSERT INTO %s (pk, ck, str_val, val) VALUES (1, 1, 'B', [2.0, 3.0, 4.0])");
        execute("DELETE from %s WHERE pk = 1 and ck = 1");

        UntypedResultSet result = execute("SELECT * FROM %s ORDER BY val ann of [2.5, 3.5, 4.5] LIMIT 1");
        assertThat(result).hasSize(1);
        assertContainsInt(result, "pk", 0);

        flush();

        result = execute("SELECT * FROM %s ORDER BY val ann of [2.5, 3.5, 4.5] LIMIT 1");
        assertThat(result).hasSize(1);
        assertContainsInt(result, "pk", 0);
    }

    // range delete won't trigger UpdateTransaction#onUpdated
    @Test
    public void rangeDeleteVectorInMemoryAndFlushTest() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, ck2 int, str_val text, val vector<float, 3>, PRIMARY KEY(pk, ck, ck2))");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");
        waitForTableIndexesQueryable();

        execute("INSERT INTO %s (pk, ck, ck2, str_val, val) VALUES (0, 0, 0, 'A', [1.0, 2.0, 3.0])");
        execute("INSERT INTO %s (pk, ck, ck2, str_val, val) VALUES (1, 1, 1, 'B', [2.0, 3.0, 4.0])");
        execute("DELETE from %s WHERE pk = 1 and ck = 1");

        UntypedResultSet result = execute("SELECT * FROM %s ORDER BY val ann of [2.5, 3.5, 4.5] LIMIT 1");
        assertThat(result).hasSize(1);
        assertContainsInt(result, "pk", 0);

        flush();

        result = execute("SELECT * FROM %s ORDER BY val ann of [2.5, 3.5, 4.5] LIMIT 1");
        assertThat(result).hasSize(1);
        assertContainsInt(result, "pk", 0);
    }

    @Test
    public void updateVectorInMemoryAndFlushTest() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, str_val text, val vector<float, 3>, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");
        waitForTableIndexesQueryable();

        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [1.0, 2.0, 3.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (1, 'B', [2.0, 3.0, 4.0])");
        execute("UPDATE %s SET val = null WHERE pk = 1");

        UntypedResultSet result = execute("SELECT * FROM %s ORDER BY val ann of [2.5, 3.5, 4.5] LIMIT 1");
        assertThat(result).hasSize(1);
        assertContainsInt(result, "pk", 0);

        flush();

        result = execute("SELECT * FROM %s ORDER BY val ann of [2.5, 3.5, 4.5] LIMIT 3");
        assertThat(result).hasSize(1);
        assertContainsInt(result, "pk", 0);
    }

    @Test
    public void deleteVectorPostFlushTest() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, str_val text, val vector<float, 3>, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");
        waitForTableIndexesQueryable();

        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [1.0, 2.0, 3.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (1, 'B', [2.0, 3.0, 4.0])");

        UntypedResultSet result = execute("SELECT * FROM %s ORDER BY val ann of [2.5, 3.5, 4.5] LIMIT 2");
        assertThat(result).hasSize(2);
        flush();

        execute("UPDATE %s SET val = null WHERE pk = 0");
        result = execute("SELECT * FROM %s ORDER BY val ann of [2.5, 3.5, 4.5] LIMIT 2");
        assertThat(result).hasSize(1);
        assertContainsInt(result, "pk", 1);

        execute("DELETE from %s WHERE pk = 1");
        result = execute("SELECT * FROM %s ORDER BY val ann of [2.5, 3.5, 4.5] LIMIT 2");
        assertThat(result).isEmpty();
        flush();

        result = execute("SELECT * FROM %s ORDER BY val ann of [2.5, 3.5, 4.5] LIMIT 2");
        assertThat(result).isEmpty();
    }

    @Test
    public void deletedInOtherSSTablesTest() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, str_val text, val vector<float, 3>, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");
        waitForTableIndexesQueryable();

        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [1.0, 2.0, 3.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (1, 'B', [2.0, 3.0, 4.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (2, 'C', [3.0, 4.0, 5.0])");

        UntypedResultSet result = execute("SELECT * FROM %s ORDER BY val ann of [0.5, 1.5, 2.5] LIMIT 1");
        assertThat(result).hasSize(1);
        assertContainsInt(result, "pk", 0);
        flush();

        execute("DELETE from %s WHERE pk = 0");
        execute("DELETE from %s WHERE pk = 1");
        result = execute("SELECT * FROM %s ORDER BY val ann of [0.5, 1.5, 2.5] LIMIT 1");
        assertThat(result).hasSize(1);
        assertContainsInt(result, "pk", 2);
    }

    @Test
    public void deletedInOtherSSTablesMultiIndexTest() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, str_val text, val vector<float, 3>, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(str_val) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");
        waitForTableIndexesQueryable();

        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [1.0, 2.0, 3.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (1, 'A', [2.0, 3.0, 4.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (2, 'A', [3.0, 4.0, 5.0])");

        UntypedResultSet result = execute("SELECT * FROM %s WHERE str_val = 'A' ORDER BY val ann of [0.5, 1.5, 2.5] LIMIT 1");
        assertThat(result).hasSize(1);
        assertContainsInt(result, "pk", 0);
        flush();

        execute("DELETE from %s WHERE pk = 0");
        execute("DELETE from %s WHERE pk = 1");
        result = execute("SELECT * FROM %s WHERE str_val = 'A' ORDER BY val ann of [0.5, 1.5, 2.5] LIMIT 1");
        assertThat(result).hasSize(1);
        assertContainsInt(result, "pk", 2);
    }

    @Test
    public void rangeDeletedInOtherSSTablesTest() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck1 int, ck2 int, str_val text, val vector<float, 3>, PRIMARY KEY(pk, ck1, ck2))");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");
        waitForTableIndexesQueryable();

        execute("INSERT INTO %s (pk, ck1, ck2, str_val, val) VALUES (0, 0, 1, 'A', [1.0, 2.0, 3.0])");
        execute("INSERT INTO %s (pk, ck1, ck2, str_val, val) VALUES (0, 0, 2, 'B', [2.0, 3.0, 4.0])");
        execute("INSERT INTO %s (pk, ck1, ck2, str_val, val) VALUES (0, 1, 3, 'C', [3.0, 4.0, 5.0])");
        execute("INSERT INTO %s (pk, ck1, ck2, str_val, val) VALUES (0, 1, 4, 'D', [3.0, 5.0, 6.0])");

        UntypedResultSet result = execute("SELECT * FROM %s ORDER BY val ann of [0.5, 1.5, 2.5] LIMIT 1");
        assertThat(result).hasSize(1);
        assertContainsInt(result, "ck1", 0);
        flush();

        execute("DELETE from %s WHERE pk = 0 and ck1 = 0");

        result = execute("SELECT * FROM %s ORDER BY val ann of [0.5, 1.5, 2.5] LIMIT 1");
        assertThat(result).hasSize(1);
        assertContainsInt(result, "ck1", 1);


        result = execute("SELECT * FROM %s ORDER BY val ann of [0.5, 1.5, 2.5] LIMIT 2");
        assertThat(result).hasSize(2);
    }

    @Test
    public void partitionDeletedInOtherSSTablesTest() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck1 int, ck2 int, str_val text, val vector<float, 3>, PRIMARY KEY(pk, ck1, ck2))");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");
        waitForTableIndexesQueryable();

        execute("INSERT INTO %s (pk, ck1, ck2, str_val, val) VALUES (0, 0, 1, 'A', [1.0, 2.0, 3.0])");
        execute("INSERT INTO %s (pk, ck1, ck2, str_val, val) VALUES (0, 0, 2, 'B', [2.0, 3.0, 4.0])");
        execute("INSERT INTO %s (pk, ck1, ck2, str_val, val) VALUES (1, 1, 3, 'C', [3.0, 4.0, 5.0])");
        execute("INSERT INTO %s (pk, ck1, ck2, str_val, val) VALUES (1, 1, 4, 'D', [3.0, 5.0, 6.0])");

        UntypedResultSet result = execute("SELECT * FROM %s ORDER BY val ann of [0.5, 1.5, 2.5] LIMIT 1");
        assertThat(result).hasSize(1);
        assertContainsInt(result, "pk", 0);
        flush();

        execute("DELETE from %s WHERE pk = 0");

        result = execute("SELECT * FROM %s ORDER BY val ann of [0.5, 1.5, 2.5] LIMIT 1");
        assertThat(result).hasSize(1);
        assertContainsInt(result, "pk", 1);


        result = execute("SELECT * FROM %s ORDER BY val ann of [0.5, 1.5, 2.5] LIMIT 2");
        assertThat(result).hasSize(2);
    }

    @Test
    public void upsertTest() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, str_val text, val vector<float, 3>, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");
        waitForTableIndexesQueryable();

        // insert row A redundantly, and row B once
        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [1.0, 2.0, 3.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [1.0, 2.0, 3.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [1.0, 2.0, 3.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [1.0, 2.0, 3.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [1.0, 2.0, 3.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (1, 'B', [2.0, 3.0, 4.0])");

        // should only see two results
        UntypedResultSet result = execute("SELECT * FROM %s ORDER BY val ann of [0.5, 1.5, 2.5] LIMIT 2");
        assertThat(result).hasSize(2);
        assertContainsInt(result, "pk", 0);
        assertContainsInt(result, "pk", 1);

        // flush, then insert A redundantly some more
        flush();
        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [1.0, 2.0, 3.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [1.0, 2.0, 3.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [1.0, 2.0, 3.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [1.0, 2.0, 3.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [1.0, 2.0, 3.0])");

        // should still only see two results
        result = execute("SELECT * FROM %s ORDER BY val ann of [0.5, 1.5, 2.5] LIMIT 2");
        assertThat(result).hasSize(2);
        assertContainsInt(result, "pk", 0);
        assertContainsInt(result, "pk", 1);

        // and again after flushing
        flush();
        result = execute("SELECT * FROM %s ORDER BY val ann of [0.5, 1.5, 2.5] LIMIT 2");
        assertThat(result).hasSize(2);
        assertContainsInt(result, "pk", 0);
        assertContainsInt(result, "pk", 1);
    }

    @Test
    public void updateTest() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, str_val text, val vector<float, 3>, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");
        waitForTableIndexesQueryable();

        // overwrite row A a bunch of times; also write row B with the same vector as a deleted A value
        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [1.0, 2.0, 3.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [2.0, 3.0, 4.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [3.0, 4.0, 5.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [4.0, 5.0, 6.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [5.0, 6.0, 7.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (1, 'B', [2.0, 3.0, 4.0])");

        // check that queries near A and B get the right row
        UntypedResultSet result = execute("SELECT * FROM %s ORDER BY val ann of [4.5, 5.5, 6.5] LIMIT 1");
        assertThat(result).hasSize(1);
        assertContainsInt(result, "pk", 0);
        result = execute("SELECT * FROM %s ORDER BY val ann of [0.5, 1.5, 2.5] LIMIT 1");
        assertThat(result).hasSize(1);
        assertContainsInt(result, "pk", 1);

        // flush, and re-check same queries
        flush();
        result = execute("SELECT * FROM %s ORDER BY val ann of [4.5, 5.5, 6.5] LIMIT 1");
        assertThat(result).hasSize(1);
        assertContainsInt(result, "pk", 0);
        result = execute("SELECT * FROM %s ORDER BY val ann of [0.5, 1.5, 2.5] LIMIT 1");
        assertThat(result).hasSize(1);
        assertContainsInt(result, "pk", 1);

        // overwite A more in the new memtable
        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [6.0, 7.0, 8.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [7.0, 8.0, 9.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [8.0, 9.0, 10.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [9.0, 10.0, 11.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [10.0, 11.0, 12.0])");

        // query near A and B again
        result = execute("SELECT * FROM %s ORDER BY val ann of [9.5, 10.5, 11.5] LIMIT 1");
        assertThat(result).hasSize(1);
        assertContainsInt(result, "pk", 0);
        result = execute("SELECT * FROM %s ORDER BY val ann of [0.5, 1.5, 2.5] LIMIT 1");
        assertThat(result).hasSize(1);
        assertContainsInt(result, "pk", 1);

        // flush, and re-check same queries
        flush();
        result = execute("SELECT * FROM %s ORDER BY val ann of [9.5, 10.5, 11.5] LIMIT 1");
        assertThat(result).hasSize(1);
        assertContainsInt(result, "pk", 0);
        result = execute("SELECT * FROM %s ORDER BY val ann of [0.5, 1.5, 2.5] LIMIT 1");
        assertThat(result).hasSize(1);
        assertContainsInt(result, "pk", 1);
    }

    @Test
    public void updateTestWithPredicate() throws Throwable
    {
        // contrived example to make sure we exercise VectorIndexSearcher.limitToTopResults
        createTable("CREATE TABLE %s (pk int, str_val text, val vector<float, 3>, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(str_val) USING 'StorageAttachedIndex'");
        waitForTableIndexesQueryable();

        // overwrite row A a bunch of times
        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [1.0, 2.0, 3.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [2.0, 3.0, 4.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [3.0, 4.0, 5.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [4.0, 5.0, 6.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [5.0, 6.0, 7.0])");

        // check that queries near A and B get the right row
        UntypedResultSet result = execute("SELECT * FROM %s WHERE str_val = 'A' ORDER BY val ann of [4.5, 5.5, 6.5] LIMIT 1");
        assertThat(result).hasSize(1);
        assertContainsInt(result, "pk", 0);
        result = execute("SELECT * FROM %s WHERE str_val = 'A' ORDER BY val ann of [0.5, 1.5, 2.5] LIMIT 1");
        assertThat(result).hasSize(1);
        assertContainsInt(result, "pk", 0);

        // flush, and re-check same queries
        flush();
        result = execute("SELECT * FROM %s WHERE str_val = 'A' ORDER BY val ann of [4.5, 5.5, 6.5] LIMIT 1");
        assertThat(result).hasSize(1);
        assertContainsInt(result, "pk", 0);
        result = execute("SELECT * FROM %s WHERE str_val = 'A' ORDER BY val ann of [0.5, 1.5, 2.5] LIMIT 1");
        assertThat(result).hasSize(1);
        assertContainsInt(result, "pk", 0);
    }

    @Test
    public void updateOtherColumnsTest() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, str_val text, val vector<float, 3>, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");
        waitForTableIndexesQueryable();

        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [1.0, 2.0, 3.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (1, 'B', [2.0, 3.0, 4.0])");
        execute("UPDATE %s SET str_val='C' WHERE pk=0");

        var result = execute("SELECT * FROM %s ORDER BY val ann of [0.5, 1.5, 2.5] LIMIT 2");
        assertThat(result).hasSize(2);
    }

    @Test
    public void updateManySSTablesTest() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, str_val text, val vector<float, 3>, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");
        waitForTableIndexesQueryable();

        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [1.0, 2.0, 3.0])");
        flush();
        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [2.0, 3.0, 4.0])");
        flush();
        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [3.0, 4.0, 5.0])");
        flush();
        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [4.0, 5.0, 6.0])");
        flush();
        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [5.0, 6.0, 7.0])");
        flush();
        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [6.0, 7.0, 8.0])");
        flush();
        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [7.0, 8.0, 9.0])");
        flush();
        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [8.0, 9.0, 10.0])");
        flush();
        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [9.0, 10.0, 11.0])");
        flush();
        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [10.0, 11.0, 12.0])");
        flush();
        execute("INSERT INTO %s (pk, str_val, val) VALUES (1, 'B', [2.0, 3.0, 4.0])");
        flush();

        var result = execute("SELECT * FROM %s ORDER BY val ann of [9.5, 10.5, 11.5] LIMIT 1");
        assertThat(result).hasSize(1);
        assertContainsInt(result, "pk", 0);
        result = execute("SELECT * FROM %s ORDER BY val ann of [0.5, 1.5, 2.5] LIMIT 1");
        assertThat(result).hasSize(1);
        assertContainsInt(result, "pk", 1);
    }


    @Test
    public void shadowedPrimaryKeyInDifferentSSTable() throws Throwable
    {
        createTable(KEYSPACE, "CREATE TABLE %s (pk int primary key, str_val text, val vector<float, 3>)");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");
        waitForTableIndexesQueryable();
        disableCompaction(KEYSPACE);

        // flush a sstable with one vector
        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [1.0, 2.0, 3.0])");
        flush();

        // flush another sstable to shadow the vector row
        execute("DELETE FROM %s where pk = 0");
        flush();

        // flush another sstable with one new vector row
        execute("INSERT INTO %s (pk, str_val, val) VALUES (1, 'B', [2.0, 3.0, 4.0])");
        flush();

        // the shadow vector has the highest score
        var result = execute("SELECT * FROM %s ORDER BY val ann of [1.0, 2.0, 3.0] LIMIT 1");
        assertThat(result).hasSize(1);
    }

    // This test intentionally has extra rows with primary keys that are above and below the
    // deleted primary key so that we do not short circuit certain parts of the shadowed key logic.
    @Test
    public void shadowedPrimaryKeyInDifferentSSTableEachWithMultipleRows() throws Throwable
    {
        createTable(KEYSPACE, "CREATE TABLE %s (pk int primary key, str_val text, val vector<float, 3>)");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");
        waitForIndexQueryable();
        disableCompaction(KEYSPACE);

        // flush a sstable with one vector
        execute("INSERT INTO %s (pk, str_val, val) VALUES (1, 'A', [1.0, 2.0, 3.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (2, 'A', [1.0, 2.0, 3.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (3, 'A', [1.0, 2.0, 3.0])");
        flush();

        // flush another sstable to shadow the vector row
        execute("INSERT INTO %s (pk, str_val, val) VALUES (1, 'A', [1.0, 2.0, 3.0])");
        execute("DELETE FROM %s where pk = 2");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (3, 'A', [1.0, 2.0, 3.0])");
        flush();

        // flush another sstable with one new vector row
        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'B', [2.0, 3.0, 4.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (4, 'B', [2.0, 3.0, 4.0])");
        flush();

        // the shadow vector has the highest score
        var result = execute("SELECT * FROM %s ORDER BY val ann of [1.0, 2.0, 3.0] LIMIT 4");
        assertThat(result).hasSize(4);
    }
}

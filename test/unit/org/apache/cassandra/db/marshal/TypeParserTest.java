/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.db.marshal;

import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.OrderPreservingPartitioner;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.SyntaxException;

import static org.apache.cassandra.Util.makeUDT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TypeParserTest
{
    @BeforeClass
    public static void initDD()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void testParse() throws ConfigurationException, SyntaxException
    {
        AbstractType<?> type;

        type = TypeParser.parse(null);
        assert type == BytesType.instance;

        type = TypeParser.parse("");
        assert type == BytesType.instance;

        type = TypeParser.parse("    ");
        assert type == BytesType.instance;

        type = TypeParser.parse("LongType");
        assert type == LongType.instance;

        type = TypeParser.parse("  LongType   ");
        assert type == LongType.instance;

        type = TypeParser.parse("LongType()");
        assert type == LongType.instance;

        type = TypeParser.parse("LongType(reversed=false)");
        assert type == LongType.instance;

        type = TypeParser.parse("LongType(reversed=true)");
        assert type == ReversedType.getInstance(LongType.instance);
        assert type.unwrap() == LongType.instance;

        type = TypeParser.parse("LongType(reversed)");
        assert type == ReversedType.getInstance(LongType.instance);
        assert type.unwrap() == LongType.instance;
    }

    @Test
    public void testParseError()
    {
        try
        {
            TypeParser.parse("y");
            fail("Should not pass");
        }
        catch (ConfigurationException e) {}
        catch (SyntaxException e) {}

        try
        {
            TypeParser.parse("LongType(reversed@)");
            fail("Should not pass");
        }
        catch (ConfigurationException e) {}
        catch (SyntaxException e) {}
    }

    @Test
    public void testParsePartitionerOrder() throws ConfigurationException, SyntaxException
    {
        assertForEachPartitioner(partitioner -> {
            AbstractType<?> type = partitioner.partitionOrdering(null);
            assertEquals(type, TypeParser.parse(type.toString()));
        });
        assertEquals(DatabaseDescriptor.getPartitioner().partitionOrdering(null), TypeParser.parse("PartitionerDefinedOrder"));
    }

    @Test
    public void testParsePartitionerOrderWithBaseType()
    {
        // default partitioner
        assertEquals(DatabaseDescriptor.getPartitioner().partitionOrdering(null), TypeParser.parse("PartitionerDefinedOrder"));

        // PartitionerDefinedOrder's base type is not composite type
        differentBaseTypeValidation(Int32Type.instance);
        // PartitionerDefinedOrder's base type is composite type
        differentBaseTypeValidation(CompositeType.getInstance(Int32Type.instance, UTF8Type.instance));
        // PartitionerDefinedOrder's base type is tuple type
        differentBaseTypeValidation(new TupleType(Lists.newArrayList(Int32Type.instance, UTF8Type.instance)));
        // PartitionerDefinedOrder's base type is ReversedType
        differentBaseTypeValidation(ReversedType.getInstance(Int32Type.instance));
        // PartitionerDefinedOrder's base type is CollectionType
        differentBaseTypeValidation(MapType.getInstance(Int32Type.instance, UTF8Type.instance, false));
    }

    @Test
    public void testParsePartitionerOrderMistMatch()
    {
        assertForEachPartitioner(partitioner -> {
            AbstractType<?> type = partitioner.partitionOrdering(null);
            if (type instanceof PartitionerDefinedOrder && !DatabaseDescriptor.getStorageCompatibilityMode().isBefore(5))
            {
                PartitionerDefinedOrder tmp = (PartitionerDefinedOrder) type;
                type = tmp.withPartitionKeyType(Int32Type.instance);
                boolean result = partitioner.partitionOrdering(null).equals(TypeParser.parse(type.toString()));
                assertFalse(result);
            }
            else
            {
                // ByteOrderedPartitioner.instance and OrderPreservingPartitioner.instance's partitionOrdering will not be PartitionerDefinedOrder
                boolean result = partitioner.partitionOrdering(null).equals(TypeParser.parse(type.toString()));
                assertTrue(result);
            }
        });
        assertEquals(DatabaseDescriptor.getPartitioner().partitionOrdering(null), TypeParser.parse("PartitionerDefinedOrder"));
    }

    @Test
    public void testParsePartitionerOrderWithErrorFormat()
    {
        assertForEachPartitioner(partitioner -> {
            AbstractType<?> type = partitioner.partitionOrdering(null);
            if (type instanceof PartitionerDefinedOrder)
            {
                // only Murmur3Partitioner and RandomPartitioner's partitionOrdering() are instanceof PartitionerDefinedOrder
                String msgPartitioner = partitioner instanceof Murmur3Partitioner ? "Murmur3Partitioner" : "RandomPartitioner";
                // error format PartitionerDefinedOrder(org.apache.cassandra.dht.Murmur3Partitioner,
                String tmpStr1 =  type.toString().replace(')', ',');
                try
                {
                    TypeParser.parse(tmpStr1);
                    fail();
                }
                catch (Throwable t)
                {
                    assertTrue(t.getCause().getMessage().contains("Syntax error parsing 'org.apache.cassandra.db.marshal.PartitionerDefinedOrder(org.apache.cassandra.dht." + msgPartitioner + ",: for msg unexpected character ','"));
                }

                // error format PartitionerDefinedOrder(org.apache.cassandra.dht.Murmur3Partitioner>
                String tmpStr2 =  type.toString().replace(')', '>');
                try
                {
                    TypeParser.parse(tmpStr2);
                    fail();
                }
                catch (Throwable t)
                {
                    assertTrue(t.getCause().getMessage().contains("Syntax error parsing 'org.apache.cassandra.db.marshal.PartitionerDefinedOrder(org.apache.cassandra.dht." + msgPartitioner + ">: for msg unexpected character '>'"));
                }

                // error format PartitionerDefinedOrder(org.apache.cassandra.dht.Murmur3Partitioner>
                String tmpStr3 =  type.toString().replace(')', ':');
                try
                {
                    TypeParser.parse(tmpStr3);
                    fail();
                }
                catch (Throwable t)
                {
                    assertTrue(t.getCause().getMessage().contains("Unable to find abstract-type class 'org.apache.cassandra.db.marshal.'"));
                }
            }
        });
        assertEquals(DatabaseDescriptor.getPartitioner().partitionOrdering(null), TypeParser.parse("PartitionerDefinedOrder"));
    }

    private void differentBaseTypeValidation(AbstractType<?> baseType)
    {
        assertForEachPartitioner(partitioner -> {
            AbstractType<?> type = partitioner.partitionOrdering(null);
            if (type instanceof PartitionerDefinedOrder && !DatabaseDescriptor.getStorageCompatibilityMode().isBefore(5))
            {
                PartitionerDefinedOrder tmp = (PartitionerDefinedOrder) type;
                type = tmp.withPartitionKeyType(baseType);
            }
            assertEquals(type, TypeParser.parse(type.toString()));
        });
    }

    public static void assertForEachPartitioner(Consumer<IPartitioner> consumer)
    {
        for (IPartitioner partitioner : new IPartitioner[] { Murmur3Partitioner.instance,
                                                             ByteOrderedPartitioner.instance,
                                                             RandomPartitioner.instance,
                                                             OrderPreservingPartitioner.instance })
        {
            consumer.accept(partitioner);
        }
    }

    @Test
    public void testTuple()
    {
        List<TupleType> tupleTypes = Arrays.asList(
//                new TupleType(Arrays.asList(UTF8Type.instance, Int32Type.instance), false),
//                new TupleType(Arrays.asList(UTF8Type.instance, Int32Type.instance), true),
//                new TupleType(Arrays.asList(UTF8Type.instance, new TupleType(Arrays.asList(Int32Type.instance, LongType.instance), false)), false),
//                new TupleType(Arrays.asList(UTF8Type.instance, new TupleType(Arrays.asList(Int32Type.instance, LongType.instance), false)), true),
                new TupleType(Arrays.asList(UTF8Type.instance, new TupleType(Arrays.asList(Int32Type.instance, LongType.instance), true)), false),
//                new TupleType(Arrays.asList(UTF8Type.instance, new TupleType(Arrays.asList(Int32Type.instance, LongType.instance), true)), true),
//                new TupleType(Arrays.asList(UTF8Type.instance, makeUDT("udt", ImmutableMap.of("a", UTF8Type.instance, "b", LongType.instance), false)), false),
//                new TupleType(Arrays.asList(UTF8Type.instance, makeUDT("udt", ImmutableMap.of("a", UTF8Type.instance, "b", LongType.instance), false)), true),
//                new TupleType(Arrays.asList(UTF8Type.instance, makeUDT("udt", ImmutableMap.of("a", UTF8Type.instance, "b", LongType.instance), true)), false),
                new TupleType(Arrays.asList(UTF8Type.instance, makeUDT("udt", ImmutableMap.of("a", UTF8Type.instance, "b", LongType.instance), true)), true)
        );

        for (TupleType tupleType : tupleTypes)
        {
            assertThat(TypeParser.parse(tupleType.toString())).describedAs(tupleType.toString()).isEqualTo(tupleType);
            assertThat(TypeParser.parse(tupleType.freeze().toString())).describedAs(tupleType.toString()).isEqualTo(tupleType.freeze());
            assertThat(TypeParser.parse(tupleType.expandUserTypes().toString())).describedAs(tupleType.toString()).isEqualTo(tupleType.expandUserTypes());
        }
    }

    @Test
    public void testParseUDT()
    {
        List<UserType> userTypes = Arrays.asList(
                makeUDT("udt", ImmutableMap.of("a", UTF8Type.instance, "b", LongType.instance), false),
                makeUDT("udt", ImmutableMap.of("a", UTF8Type.instance, "b", LongType.instance), true),
                makeUDT("udt", ImmutableMap.of("a", UTF8Type.instance, "b", new TupleType(Arrays.asList(Int32Type.instance, LongType.instance), false)), false),
                makeUDT("udt", ImmutableMap.of("a", UTF8Type.instance, "b", new TupleType(Arrays.asList(Int32Type.instance, LongType.instance), false)), true),
                makeUDT("udt", ImmutableMap.of("a", UTF8Type.instance, "b", new TupleType(Arrays.asList(Int32Type.instance, LongType.instance), true)), false),
                makeUDT("udt", ImmutableMap.of("a", UTF8Type.instance, "b", new TupleType(Arrays.asList(Int32Type.instance, LongType.instance), true)), true),
                makeUDT("udt", ImmutableMap.of("a", UTF8Type.instance, "b", makeUDT("udt2", ImmutableMap.of("a", UTF8Type.instance, "b", LongType.instance), false)), false),
                makeUDT("udt", ImmutableMap.of("a", UTF8Type.instance, "b", makeUDT("udt2", ImmutableMap.of("a", UTF8Type.instance, "b", LongType.instance), false)), true),
                makeUDT("udt", ImmutableMap.of("a", UTF8Type.instance, "b", makeUDT("udt2", ImmutableMap.of("a", UTF8Type.instance, "b", LongType.instance), true)), false),
                makeUDT("udt", ImmutableMap.of("a", UTF8Type.instance, "b", makeUDT("udt2", ImmutableMap.of("a", UTF8Type.instance, "b", LongType.instance), true)), true)
        );

        for (UserType userType : userTypes)
        {
            assertEquals(userType, TypeParser.parse(userType.toString()));
            assertEquals(userType.freeze(), TypeParser.parse(userType.freeze().toString()));
            assertEquals(userType.expandUserTypes(), TypeParser.parse(userType.expandUserTypes().toString()));
        }
    }
}

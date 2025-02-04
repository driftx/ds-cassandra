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

package org.apache.cassandra.transport.messages;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.Constants;
import org.apache.cassandra.cql3.FieldIdentifier;
import org.apache.cassandra.cql3.ResultSet;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.DynamicCompositeType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.ReversedType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.TupleType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.transport.Envelope;
import org.apache.cassandra.transport.Event;
import org.apache.cassandra.transport.messages.ResultMessage.Rows;
import org.apache.cassandra.transport.messages.ResultMessage.SchemaChange;
import org.apache.cassandra.transport.messages.ResultMessage.SetKeyspace;
import org.apache.cassandra.utils.MD5Digest;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

public class ResultMessageTest
{
    private static ByteBuffer bb(String str)
    {
        return UTF8Type.instance.decompose(str);
    }

    private static FieldIdentifier field(String field)
    {
        return FieldIdentifier.forQuoted(field);
    }
    @Test
    public void testSchemaChange()
    {
        Event.SchemaChange scEvent = new Event.SchemaChange(Event.SchemaChange.Change.CREATED, Event.SchemaChange.Target.TABLE, "ks", "test");
        SchemaChange sc1 = new SchemaChange(scEvent);
        assertThat(sc1.change.keyspace).isEqualTo("ks");
        SchemaChange sc2 = overrideKeyspace(sc1);
        assertThat(sc2.change.keyspace).isEqualTo("ks_123");
    }

    @Test
    public void testPrepared()
    {
        ColumnSpecification cs1 = new ColumnSpecification("ks1", "cf1", new ColumnIdentifier("a", true), Int32Type.instance);
        ColumnSpecification cs2 = new ColumnSpecification("ks2", "cf2", new ColumnIdentifier("b", true), Int32Type.instance);
        ResultSet.PreparedMetadata preparedMetadata = new ResultSet.PreparedMetadata(Arrays.asList(cs1, cs2), new short[]{ 2, 4, 6 });
        ResultSet.ResultMetadata resultMetadata = new ResultSet.ResultMetadata(Arrays.asList(cs1, cs2));
        ResultMessage.Prepared p1 = new ResultMessage.Prepared(mock(MD5Digest.class), mock(MD5Digest.class), preparedMetadata, resultMetadata);
        ResultMessage.Prepared p2 = overrideKeyspace(p1);
        assertThat(p2.metadata.names.stream().map(cs -> cs.ksName)).containsExactly("ks1_123", "ks2_123");
        assertThat(p2.resultMetadata.names.stream().map(cs -> cs.ksName)).containsExactly("ks1_123", "ks2_123");
    }

    @Test
    public void testSetKeyspace()
    {
        SetKeyspace sk1 = new SetKeyspace("ks");
        SetKeyspace sk2 = overrideKeyspace(sk1);
        assertThat(sk2.keyspace).isEqualTo("ks_123");
    }

    @Test
    public void testRows()
    {
        FieldIdentifier f1 = field("f1");  // has field position 0
        FieldIdentifier f2 = field("f2");  // has field position 1

        List<AbstractType<?>> allTypes = new ArrayList<>();
        UserType udt = new UserType("ks1",
                                    bb("myType"),
                                    asList(f1, f2),
                                    asList(Int32Type.instance, UTF8Type.instance),
                                    false);
        allTypes.add(udt);
        ListType lt = ListType.getInstance(udt, false);
        allTypes.add(lt);
        MapType mt = MapType.getInstance(Int32Type.instance, udt, false);
        allTypes.add(mt);
        SetType st = SetType.getInstance(udt, false);
        allTypes.add(st);
        CompositeType ct = CompositeType.getInstance(Int32Type.instance, UTF8Type.instance);
        allTypes.add(ct);
        DynamicCompositeType dct = DynamicCompositeType.getInstance(ImmutableMap.of((byte)8, Int32Type.instance));
        allTypes.add(dct);
        TupleType tt = new TupleType(asList(Int32Type.instance, udt));
        allTypes.add(tt);
        allTypes.add(Int32Type.instance);
        AbstractType<?> rt = ReversedType.getInstance(udt);
        allTypes.add(rt);

        ColumnSpecification cs1 = new ColumnSpecification("ks1", "cf1", new ColumnIdentifier("a", true), Int32Type.instance);
        ColumnSpecification cs2 = new ColumnSpecification("ks1", "cf1", new ColumnIdentifier("b", true), Int32Type.instance);
        ColumnSpecification cs3 = new ColumnSpecification("ks1", "cf1", new ColumnIdentifier("c", true), udt);
        ColumnSpecification cs4 = new ColumnSpecification("ks1", "cf1", new ColumnIdentifier("d", true), tt);
        ColumnSpecification cs5 = new ColumnSpecification("ks1", "cf1", new ColumnIdentifier("e", true), rt);
        ColumnSpecification cs6 = new ColumnSpecification("ks1", "cf1", new ColumnIdentifier("f", true), lt);
        ColumnSpecification cs7 = new ColumnSpecification("ks1", "cf1", new ColumnIdentifier("g", true), mt);
        ColumnSpecification cs8 = new ColumnSpecification("ks1", "cf1", new ColumnIdentifier("h", true), st);
        ColumnSpecification cs9 = new ColumnSpecification("ks1", "cf1", new ColumnIdentifier("i", true), ct);
        ColumnSpecification cs10 = new ColumnSpecification("ks1", "cf1", new ColumnIdentifier("j", true), dct);

        ResultSet.ResultMetadata resultMetadata = new ResultSet.ResultMetadata(Arrays.asList(cs1, cs2, cs3, cs4, cs5, cs6, cs7, cs8, cs9, cs10));
        ResultSet rs = new ResultSet(resultMetadata, mock(List.class));
        Rows r1 = new Rows(rs);
        checkRows(r1);
        Rows r2 = overrideKeyspace(r1);
        checkRows(r2);
        assertThat(r2.result.metadata.names.stream().map(cs -> cs.ksName)).allMatch(k -> k.equals("ks1_123"));
        assertThat(r2.result.metadata.getResultMetadataId()).isNotSameAs(r1.result.metadata.getResultMetadataId());

        //Also Test no change path
        List<AbstractType<?>> newTypes = allTypes.stream().map(t -> t.overrideKeyspace(s -> s)).collect(Collectors.toList());
        assertThat(allTypes).isEqualTo(newTypes);
    }

    private <T extends ResultMessage<T>> T overrideKeyspace(ResultMessage<T> rm)
    {
        T rm2 = rm.withOverriddenKeyspace(Constants.IDENTITY_STRING_MAPPER);
        assertThat(rm2).isSameAs(rm);
        T rm3 = rm2.withOverriddenKeyspace(ks -> ks);
        assertThat(rm3).isSameAs(rm);
        rm.setWarnings(mock(List.class));
        rm.setCustomPayload(mock(Map.class));
        rm.setSource(mock(Envelope.class));
        rm.setStreamId(123);
        T rm4 = rm3.withOverriddenKeyspace(ks -> ks + "_123");
        assertThat(rm4).isNotSameAs(rm);
        assertThat(rm4.getWarnings()).isSameAs(rm.getWarnings());
        assertThat(rm4.getCustomPayload()).isSameAs(rm.getCustomPayload());
        assertThat(rm4.getSource()).isSameAs(rm.getSource());
        assertThat(rm4.getStreamId()).isSameAs(rm.getStreamId());

        return rm4;
    }

    void checkRows(ResultMessage.Rows r)
    {
        String ksName = r.result.metadata.names.get(0).ksName;
        for (ColumnSpecification cf : r.result.metadata.names)
            checkType(cf.type, ksName);
    }

    void checkType(AbstractType<?> type, String keyspaceName)
    {
        if (type.isUDT())
        {
            UserType ut = (UserType) type;
            assertThat(ut.keyspace).isEqualTo(keyspaceName);

            for (int i = 0; i < ut.size(); i++)
                checkType(ut.type(i), keyspaceName);
        }
        else if (type.isTuple())
        {
            TupleType tt = (TupleType) type;

            for (int i = 0; i < tt.size(); i++)
                checkType(tt.type(i), keyspaceName);
        }
        else if (type.isReversed())
        {
            checkType(type.unwrap(), keyspaceName);
        }
        else if (type.isCollection())
        {
            CollectionType<?> ct = (CollectionType<?>) type;
            checkType(ct.nameComparator(), keyspaceName);
            checkType(ct.valueComparator(), keyspaceName);
        }
    }
}

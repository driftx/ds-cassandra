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

package org.apache.cassandra.db.marshal;

import java.nio.ByteBuffer;

import com.esri.core.geometry.ogc.OGCPolygon;
import org.apache.cassandra.db.marshal.geometry.GeometricType;
import org.apache.cassandra.db.marshal.geometry.Polygon;

public class PolygonType extends AbstractGeometricType<Polygon>
{
    public static final PolygonType instance = new PolygonType();

    private static final ByteBuffer MASKED_VALUE = new Polygon((OGCPolygon) OGCPolygon.fromText("POLYGON EMPTY")).asWellKnownBinary();

    public PolygonType()
    {
        super(GeometricType.POLYGON);
    }

    @Override
    public ByteBuffer getMaskedValue()
    {
        return MASKED_VALUE;
    }
}

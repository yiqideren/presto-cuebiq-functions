/*
 * Copyright 2016 Cuebiq Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cuebiq.presto.scalar;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.DoubleType;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.junit.Assert;
import org.junit.Test;

public class GeographicFunctionsTest {

    @Test
    public void testHaversineZeroDistance() {
        double haversine = GeographicFunctions.haversine(0, 0, 0, 0);
        Assert.assertEquals(0, haversine, Double.MIN_VALUE);

        double haversine2 = GeographicFunctions.haversine(10, 20, 10, 20);
        Assert.assertEquals(0, haversine2, Double.MIN_VALUE);
    }

    @Test
    public void testHaversineStandardCase() {
        double haversine2 = GeographicFunctions.haversine(13, 22, 10, 20);
        Assert.assertEquals(398444.295394513, haversine2, Double.MIN_VALUE);
    }

    @Test
    public void testHaversineOverBoundaryParams() {
        double haversine2 = GeographicFunctions.haversine(181, 20, 181, 21);
        Assert.assertEquals(111177.99068882648, haversine2, Double.MIN_VALUE);
    }

    @Test
    public void testGeoHashEncode() {

        Slice geohash = GeographicFunctions.geohash_encode(45.0, 9.0, 9);
        Assert.assertEquals("u0n2hb185", geohash.toStringUtf8());
    }

    @Test
    public void testGeoHashDecode() {

        Block coordinates = GeographicFunctions.geohash_decode(Slices.utf8Slice("u0n2hb185"));
        Assert.assertEquals(45.00002145767212, DoubleType.DOUBLE.getDouble(coordinates,0), Double.MIN_VALUE);
        Assert.assertEquals(9.000012874603271, DoubleType.DOUBLE.getDouble(coordinates,1), Double.MIN_VALUE);
    }
}

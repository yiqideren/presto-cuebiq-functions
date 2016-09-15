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

import org.junit.Assert;
import org.junit.Test;

public class GeohashEncodeTest {


    @Test
    public void testHaversineZeroDistance()
    {
        double haversine = Haversine.haversine(0, 0, 0, 0);
        Assert.assertEquals(0,haversine,Double.MIN_VALUE);

        double haversine2 = Haversine.haversine(10, 20, 10, 20);
        Assert.assertEquals(0,haversine2,Double.MIN_VALUE);
    }

    @Test
    public void testHaversineStandardCase()
    {
        double haversine2 = Haversine.haversine(13, 22, 10, 20);
        Assert.assertEquals(398444.295394513,haversine2,Double.MIN_VALUE);
    }


    @Test
    public void testHaversineOverBoundaryParams()
    {
        double haversine2 = Haversine.haversine(181, 20, 181, 21);
        Assert.assertEquals(111177.99068882648,haversine2,Double.MIN_VALUE);
    }



}

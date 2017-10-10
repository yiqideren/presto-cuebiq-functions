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

import com.esri.core.geometry.OperatorContains;
import com.esri.core.geometry.Point;
import com.esri.core.geometry.Polygon;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlNullable;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.StandardTypes;

import javax.annotation.Nullable;

/**
 * as the description states,
 * this function allows to check whether a point is inside or outside a polygon.
 */
@Description("returns true if point is in polygon")
@ScalarFunction("poly_contains")
public class PolyContains {

    @SqlType(StandardTypes.BOOLEAN)
    @Nullable
    @SqlNullable
    public static Boolean contains(
            @SqlType("array(double)") Block arrayBlock,
            @SqlType(StandardTypes.DOUBLE) double lng,
            @SqlType(StandardTypes.DOUBLE) double lat) {
        double[] array = new double[arrayBlock.getPositionCount()];
        Polygon poly = new Polygon();

        for (int i = 0; i < arrayBlock.getPositionCount(); i++) {

            if (arrayBlock.isNull(i)) {
                continue;
            }
            array[i] = arrayBlock.getSlice(i, 0, arrayBlock.getSliceLength(i)).getDouble(0);
        }

        poly.startPath(array[0], array[1]);
        for (int i = 2; i < array.length; i += 2) {
            poly.lineTo(array[i], array[i + 1]);
        }
        return OperatorContains.local().execute(poly, new Point(lng, lat), null, null);
    }

}

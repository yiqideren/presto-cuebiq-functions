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

import com.facebook.presto.operator.Description;
import com.facebook.presto.operator.scalar.annotations.ScalarFunction;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.type.SqlType;

@Description("haversine distance. params: lat1, lng1, lat2, lng2")
@ScalarFunction("haversine")
public class Haversine {


    @SqlType(StandardTypes.DOUBLE)
    public static double haversine(@SqlType(StandardTypes.DOUBLE) double lat1, @SqlType(StandardTypes.DOUBLE) double lng1, @SqlType(StandardTypes.DOUBLE) double lat2, @SqlType(StandardTypes.DOUBLE) double lng2) {
        double dLat = Math.toRadians(lat2 - lat1);
        double dLng = Math.toRadians(lng2 - lng1);
        double a = Math.sin(dLat / 2.0D) * Math.sin(dLat / 2.0D) + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2)) * Math.sin(dLng / 2.0D) * Math.sin(dLng / 2.0D);
        double c = 2.0D * Math.atan2(Math.sqrt(a), Math.sqrt(1.0D - a));
        return 6371000.0D * c;
    }
}

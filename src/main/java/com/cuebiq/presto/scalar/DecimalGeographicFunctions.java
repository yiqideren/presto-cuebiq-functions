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
import com.facebook.presto.operator.scalar.annotations.TypeParameter;
import com.facebook.presto.operator.scalar.annotations.TypeParameterContainer;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.Decimals;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.type.SqlType;
import io.airlift.slice.Slice;

import javax.annotation.Nullable;
import java.math.BigDecimal;

@Description("geoHash bigdecimals. params : lat,lng, precision")
@ScalarFunction("geohash_encode_dec")
public class DecimalGeographicFunctions {

    @SqlType(StandardTypes.VARCHAR)
    @TypeParameterContainer({@TypeParameter("decimal(lat_precision, lat_scale)")
            , @TypeParameter("decimal(lng_precision, lng_scale)")}
    )
    @Nullable
    public static Slice geohash_encode_dec(
            @TypeParameter("decimal(lat_precision, lat_scale)") DecimalType latParameter,
            @TypeParameter("decimal(lng_precision, lng_scale)") DecimalType lngParameter,
            @SqlType("decimal(lat_precision, lat_scale)") Slice lat,
            @SqlType("decimal(lng_precision, lng_scale)") Slice lng,
            @SqlType(StandardTypes.INTEGER) long precision) {


        BigDecimal biglat = new BigDecimal(Decimals.decodeUnscaledValue(lat), latParameter.getScale());
        BigDecimal bigLng = new BigDecimal(Decimals.decodeUnscaledValue(lng), lngParameter.getScale());

        return GeohashEncode.geohash_encode(biglat.doubleValue(), bigLng.doubleValue(), precision);
    }


}

package com.cuebiq.presto.scalar;

import com.facebook.presto.operator.Description;
import com.facebook.presto.operator.scalar.ScalarFunction;
import com.facebook.presto.operator.scalar.TypeParameter;
import com.facebook.presto.operator.scalar.TypeParameterContainer;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.Decimals;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.type.SqlType;
import io.airlift.slice.Slice;

import javax.annotation.Nullable;
import java.math.BigDecimal;

@Description("geoHash bigdecimals. params : lat,lng, precision")
@ScalarFunction("geohash_encode")
public class DecimalGeographicFunctions {

    @SqlType(StandardTypes.VARCHAR)
    @TypeParameterContainer({@TypeParameter("decimal(lat_precision, lat_scale)")
            , @TypeParameter("decimal(lng_precision, lng_scale)")}
    )
    @Nullable
    public static Slice geohash_encode_decimal(
            @TypeParameter("decimal(lat_precision, lat_scale)") DecimalType latParameter,
            @TypeParameter("decimal(lng_precision, lng_scale)") DecimalType lngParameter,
            @SqlType("decimal(lat_precision, lat_scale)") Slice lat,
            @SqlType("decimal(lng_precision, lng_scale)") Slice lng,
            @SqlType(StandardTypes.INTEGER) long precision) {


        BigDecimal biglat = new BigDecimal(Decimals.decodeUnscaledValue(lat), latParameter.getScale());
        BigDecimal bigLng = new BigDecimal(Decimals.decodeUnscaledValue(lng), lngParameter.getScale());

        return GeographicFunctions.geohash_encode(biglat.doubleValue(), bigLng.doubleValue(), precision);
    }


}

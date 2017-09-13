package com.cuebiq.presto.scalar;

import com.facebook.presto.operator.Description;
import com.facebook.presto.operator.scalar.annotations.ScalarFunction;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.type.SqlType;
import com.github.davidmoten.geo.GeoHash;
import io.airlift.slice.Slice;

@ScalarFunction("geohash_decode_lat")
@Description("geoHash. params: geohash")
public class GeoHashDecodeLat {

    @SqlType(StandardTypes.DOUBLE)
    public static double geohash_decode_lat(@SqlType(StandardTypes.VARCHAR) Slice geohash) {

        return GeoHash.decodeHash(geohash.toStringUtf8()).getLat();
    }
}

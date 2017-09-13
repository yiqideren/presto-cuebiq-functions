package com.cuebiq.presto.scalar;

import com.facebook.presto.operator.Description;
import com.facebook.presto.operator.scalar.annotations.ScalarFunction;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.type.SqlType;
import com.github.davidmoten.geo.GeoHash;
import com.github.davidmoten.geo.LatLong;
import io.airlift.slice.Slice;

import static com.facebook.presto.spi.type.DoubleType.DOUBLE;

@ScalarFunction("geohash_decode")
@Description("geoHash. params: geohash")
public class GeoHashDecode {

    @SqlType("array(double)")
    public static Block geohash_decode(@SqlType(StandardTypes.VARCHAR) Slice geohash) {

        BlockBuilder blockBuilder = DOUBLE.createBlockBuilder(new BlockBuilderStatus(), 2);
        LatLong coordinates = GeoHash.decodeHash(geohash.toStringUtf8());
        DOUBLE.writeDouble(blockBuilder, coordinates.getLat());
        DOUBLE.writeDouble(blockBuilder, coordinates.getLon());
        return blockBuilder.build();
    }
}

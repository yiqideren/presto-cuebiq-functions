package com.cuebiq.presto.scalar;

import com.facebook.presto.metadata.FunctionListBuilder;
import com.facebook.presto.type.TypeRegistry;
import org.junit.Test;

/**
 * Created by emanuelesan on 17/06/16.
 */
public class DecimalGeohashEncodeTest {

    @Test
    public void testFunctionCreation()
    {

        TypeRegistry typeRegistry = new TypeRegistry();
        FunctionListBuilder builder = new FunctionListBuilder();

        builder.scalar(DecimalGeographicFunctions.class);
    }

}
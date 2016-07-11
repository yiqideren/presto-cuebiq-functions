package com.cuebiq.presto.scalar;

import com.facebook.presto.metadata.FunctionListBuilder;
import com.facebook.presto.type.TypeRegistry;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.*;

/**
 * Created by emanuelesan on 17/06/16.
 */
public class DecimalGeographicFunctionsTest {

    @Test
    public void testFunctionCreation()
    {

        TypeRegistry typeRegistry = new TypeRegistry();
        FunctionListBuilder builder = new FunctionListBuilder(typeRegistry);

        builder.scalar(DecimalGeographicFunctions.class);
    }

}
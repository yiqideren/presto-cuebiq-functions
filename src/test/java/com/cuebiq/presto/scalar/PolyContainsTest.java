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

import com.facebook.presto.metadata.FunctionListBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.LongArrayBlock;
import com.facebook.presto.spi.block.LongArrayBlockBuilder;
import com.facebook.presto.type.TypeRegistry;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class PolyContainsTest {
    @Test
    public void testFunctionCreation() {

        TypeRegistry typeRegistry = new TypeRegistry();
        FunctionListBuilder builder = new FunctionListBuilder();

        builder.scalar(PolyContains.class);
    }

    @Test
    public void testPoly_contains() throws Exception {

        double[] poly = new double[]{
                45, 9.5,
                45.5, 9.5,
                45.5, 9,
                46, 9,
                46, 10,
                45, 10
        };

        LongArrayBlock blockPoly = toBlock(poly);
        assertFalse(PolyContains.contains(blockPoly, 6, 3));
        assertFalse(PolyContains.contains(blockPoly, 45, 9));
        assertTrue(PolyContains.contains(blockPoly, 45.7, 9.7));

    }

    private static LongArrayBlock toBlock(double[] poly) {

        LongArrayBlockBuilder blockBuilder = new LongArrayBlockBuilder(new BlockBuilderStatus(), poly.length);
        for (double d : poly) {
            blockBuilder.writeLong(Double.doubleToLongBits(d));
            blockBuilder.closeEntry();
        }
        return ((LongArrayBlock) blockBuilder.build());
    }
}
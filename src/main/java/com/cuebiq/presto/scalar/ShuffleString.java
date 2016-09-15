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
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.util.Random;

@ScalarFunction("shuffle_string")
@Description("shuffles using a pseudo random algorithm.")
public class ShuffleString {

    @SqlType(StandardTypes.VARCHAR)
    public static Slice shuffle_string(@SqlType(StandardTypes.VARCHAR) Slice string) {
        String id = string.toStringUtf8();
        Random rnd = new Random(id.charAt(0));
        byte[] bytes = id.getBytes();
        for (int i = bytes.length; i > 1; i--) {
            swap(bytes, i - 1, rnd.nextInt(i));
        }
        return Slices.wrappedBuffer(bytes);

    }

    /**
     * Swaps the two specified elements in the byte array.
     */
    private static void swap(byte[] arr, int i, int j) {
        byte tmp = arr[i];
        arr[i] = arr[j];
        arr[j] = tmp;
    }

}

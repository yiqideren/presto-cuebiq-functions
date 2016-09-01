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
import org.apache.commons.codec.digest.DigestUtils;

import java.util.Random;

/**
 * as of .147 version,
 * PrestoDB doesn't offer hashing functions returning String type result.
 * these methods are a substitution for those methods.
 * returned String uses UTF-8 charset.
 */
public class HashingFunctions {

    private HashingFunctions() {
    }

    @Description("hashes with sha_256")
    @ScalarFunction
    @SqlType(StandardTypes.VARCHAR)
    public static Slice sha_256(@SqlType(StandardTypes.VARCHAR) Slice string) {

        return Slices.utf8Slice(DigestUtils.sha256Hex(string.toStringUtf8()));

    }

    @Description("hashes with md5")
    @ScalarFunction
    @SqlType(StandardTypes.VARCHAR)
    public static Slice md_5(@SqlType(StandardTypes.VARCHAR) Slice string) {

        return Slices.utf8Slice(DigestUtils.md5Hex(string.toStringUtf8()));

    }

    @Description("shuffles using a pseudo random algorithm.")
    @ScalarFunction
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

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

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.junit.Assert;
import org.junit.Test;

public class HashingFunctionsTest {


    @Test
    public void sha_256()
    {

        Slice hashedString = Sha256.sha_256(Slices.utf8Slice("test"));
        Assert.assertEquals("9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08",hashedString.toStringUtf8());
    }


    @Test
    public void md_5()
    {
        Slice hashedString = HashingFunctions.md_5(Slices.utf8Slice("test"));
        Assert.assertEquals("098f6bcd4621d373cade4e832627b4f6",hashedString.toStringUtf8());
    }

    @Test
    public void shuffle_string()
    {
        Slice shuffledString = ShuffleString.shuffle_string(Slices.utf8Slice("test"));
        Assert.assertEquals("ttes",shuffledString.toStringUtf8());

    }
}

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
package com.cuebiq.presto;


import com.cuebiq.presto.scalar.DatesFunctions;
import com.cuebiq.presto.scalar.GeographicFunctions;
import com.cuebiq.presto.scalar.HashingFunctions;
import com.cuebiq.presto.scalar.PolyContains;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.type.TypeManager;
import com.google.inject.Inject;


import java.util.*;

import static com.google.common.base.Preconditions.checkNotNull;

public class CuebiqPlugin implements Plugin {

    private TypeManager typeManager;

    @Inject
    @SuppressWarnings("unused")
    public void setTypeManager(TypeManager typeManager) {
        this.typeManager = checkNotNull(typeManager, "typeManager is null");
    }

    @Override
    public Set<Class<?>> getFunctions() {
        List<Class<?>> classes1 = Arrays.asList(
                PolyContains.class,
                HashingFunctions.class,
                GeographicFunctions.class,
                DatesFunctions.class
        );
        return new HashSet<>(classes1);

    }

}

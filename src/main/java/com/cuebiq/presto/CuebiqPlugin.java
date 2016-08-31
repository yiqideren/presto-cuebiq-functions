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

import com.facebook.presto.metadata.FunctionFactory;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;

import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public class CuebiqPlugin implements com.facebook.presto.spi.Plugin {

    private TypeManager typeManager;

    @Override
    public void setOptionalConfig(Map<String, String> optionalConfig) {

    }

    @Inject
    @SuppressWarnings("unused")
    public void setTypeManager(TypeManager typeManager) {
        this.typeManager = checkNotNull(typeManager, "typeManager is null");
    }

    @Override
    public <T> List<T> getServices(Class<T> type) {
        if (type == FunctionFactory.class) {
            return ImmutableList.of(type.cast(new UdfFactory(typeManager)));
        }

        return ImmutableList.of();
    }

}

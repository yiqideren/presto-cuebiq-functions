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
import com.facebook.presto.metadata.FunctionListBuilder;
import com.facebook.presto.metadata.SqlFunction;
import com.facebook.presto.operator.aggregation.AggregationFunction;
import com.facebook.presto.operator.aggregation.ArrayAggregationFunction;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class UdfFactory implements FunctionFactory {

    private final TypeManager typeManager;

    public UdfFactory(TypeManager tm) {
        this.typeManager = tm;
    }

    @Override
    public List<SqlFunction> listFunctions() {
        FunctionListBuilder builder = new FunctionListBuilder(typeManager);
        try {
            List<Class<?>> classes = getFunctionClasses();
            addFunctions(builder, classes);
        } catch (IOException e) {
            System.out.println("Could not load classes from jar file: " + e);
            return ImmutableList.of();
        }

        return builder.getFunctions();
    }

    private void addFunctions(FunctionListBuilder builder, List<Class<?>> classes) {
        for (Class<?> clazz : classes) {
            if (ArrayAggregationFunction.class.isAssignableFrom(clazz)) {
                try {
                    builder.function((ArrayAggregationFunction) clazz.newInstance());
                } catch (InstantiationException | IllegalAccessException e) {
                    System.out.println(String.format("Could not add %s, exception: %s, stack: %s", clazz.getCanonicalName(), e,ExceptionUtils.getStackTrace( e)));
                }
            } else {
                String canonicalName = clazz.getCanonicalName();
                if (canonicalName == null) {
                    continue;
                }

                if (canonicalName.startsWith("com.cuebiq.presto.scalar")) {
                    System.out.println("registering " + canonicalName);
                    try {
                        builder.scalar(clazz);
                    } catch (Exception e) {
                        if (e.getCause() instanceof IllegalAccessException) {
                            // This is alright, must be helper classes
                        } else {
                            System.out.println(String.format("Could not add %s, exception: %s, stack: %s", canonicalName, e, ExceptionUtils.getStackTrace(e)));
                        }
                    }
                } else if (canonicalName.startsWith("com.cuebiq.presto.aggregation")) {
                    AggregationFunction aggregationAnnotation = clazz.getAnnotation(AggregationFunction.class);
                    if (aggregationAnnotation == null) {
                        continue;
                    }
                    try {
                        builder.aggregate(clazz);
                    } catch (Exception e) {
                        System.out.println(String.format("Could not add %s, exception: %s, stack: %s", canonicalName, e,ExceptionUtils.getStackTrace( e)));
                    }
                }
            }
        }
    }

    private List<Class<?>> getFunctionClasses()
            throws IOException {
        List<Class<?>> classes = new ObjectArrayList<>();
        String classResource = this.getClass().getCanonicalName().replace(".", "/") + ".class";
        String jarURLFile = Thread.currentThread().getContextClassLoader().getResource(classResource).getFile();
        int jarEnd = jarURLFile.indexOf('!');
        String jarLocation = jarURLFile.substring(0, jarEnd); // This is in URL format, convert once more to get actual file location
        jarLocation = new URL(jarLocation).getFile();

        ZipInputStream zip = new ZipInputStream(new FileInputStream(jarLocation));
        for (ZipEntry entry = zip.getNextEntry(); entry != null; entry = zip.getNextEntry()) {
            if (entry.getName().endsWith(".class") && !entry.isDirectory()) {
                String className = entry.getName().replace("/", "."); // This still has .class at the end
                className = className.substring(0, className.length() - 6); // remvove .class from end
                try {
                    classes.add(Class.forName(className));
                } catch (ClassNotFoundException e) {
                    System.out.println(String.format("Could not load class %s, Exception: %s", className, e));
                }
            }
        }
        return classes;
    }
}

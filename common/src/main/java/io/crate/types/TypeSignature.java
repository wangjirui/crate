/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.types;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

import static java.lang.String.format;

public class TypeSignature {

    public static TypeSignature parseTypeSignature(String signature) {
        if (!signature.contains("(")) {
            return new TypeSignature(signature);
        }

        String baseName = null;
        List<TypeSignatureParameter> parameters = new ArrayList<>();
        int parameterStart = -1;
        int bracketCount = 0;

        for (int i = 0; i < signature.length(); i++) {
            char c = signature.charAt(i);
            if (c == '(') {
                if (bracketCount == 0) {
                    assert baseName == null : "Expected baseName to be null";
                    baseName = signature.substring(0, i);
                    parameterStart = i + 1;
                }
                bracketCount++;
            } else if (c == ')') {
                bracketCount--;
                if (bracketCount == 0) {
                    assert parameterStart >= 0 : "Expected parameter start to be >= 0";
                    parameters.add(parseTypeSignatureParameter(signature, parameterStart, i));
                    parameterStart = i + 1;
                    if (i == signature.length() - 1) {
                        return new TypeSignature(baseName, parameters);
                    }
                }
            } else if (c == ',') {
                if (bracketCount == 1) {
                    assert parameterStart >= 0 : "Expected parameter start to be >= 0";
                    parameters.add(parseTypeSignatureParameter(signature, parameterStart, i));
                    parameterStart = i + 1;
                }
            }
        }

        throw new IllegalArgumentException(format(Locale.ENGLISH, "Bad type signature: '%s'", signature));
    }

    private static TypeSignatureParameter parseTypeSignatureParameter(String signature, int begin, int end) {
        String parameterName = signature.substring(begin, end).trim();
        return TypeSignatureParameter.of(parseTypeSignature(parameterName));
    }

    private final String base;
    private final List<TypeSignatureParameter> parameters;

    public TypeSignature(String base) {
        this(base, Collections.emptyList());
    }

    public TypeSignature(String base, List<TypeSignatureParameter> parameters) {
        this.base = base;
        this.parameters = parameters;
    }

    public String getBase() {
        return base;
    }

    public List<TypeSignatureParameter> getParameters() {
        return parameters;
    }

    @Override
    public String toString() {
        if (parameters.isEmpty()) {
            return base;
        }

        StringBuilder typeName = new StringBuilder(base);
        typeName.append("(").append(parameters.get(0));
        for (int i = 1; i < parameters.size(); i++) {
            typeName.append(",").append(parameters.get(i));
        }
        typeName.append(")");
        return typeName.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TypeSignature that = (TypeSignature) o;
        return base.equals(that.base) &&
               parameters.equals(that.parameters);
    }

    @Override
    public int hashCode() {
        return Objects.hash(base, parameters);
    }
}

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

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static io.crate.types.TypeSignature.parseTypeSignature;

public final class TypeCompatibility {

    private static TypeCompatibility compatible(DataType<?> commonSuperType, boolean coercible) {
        return new TypeCompatibility(commonSuperType, coercible);
    }

    private static TypeCompatibility incompatible() {
        return new TypeCompatibility(null, false);
    }

    @Nullable
    public static DataType<?> getCommonSuperType(DataType<?> firstType, DataType<?> secondType) {
        TypeCompatibility compatibility = compatibility(firstType, secondType);
        if (!compatibility.isCompatible()) {
            return null;
        }
        return compatibility.getCommonSuperType();
    }

    private static TypeCompatibility compatibility(DataType<?> fromType, DataType<?> toType) {
        if (fromType.equals(toType)) {
            return compatible(toType, true);
        }

        if (fromType.equals(UndefinedType.INSTANCE)) {
            return compatible(toType, true);
        }

        if (toType.equals(UndefinedType.INSTANCE)) {
            return compatible(fromType, false);
        }

        // If given types share the same base, e.g. arrays, parameter types must be compatible.
        String fromTypeBaseName = fromType.getTypeSignature().getBase();
        String toTypeBaseName = toType.getTypeSignature().getBase();
        if (fromTypeBaseName.equals(toTypeBaseName)) {
            if (isCovariantParametrizedType(fromType)) {
                return typeCompatibilityForCovariantParametrizedType(fromType, toType);
            }
            return compatible(fromType, false);
        }

        // Use possible common super type (safe conversion)
        DataType<?> commonSuperType = convertTypeByPrecedence(fromType, toType);
        if (commonSuperType != null) {
            return compatible(commonSuperType, commonSuperType.equals(toType));
        }

        // Try to force conversion, first to the target type or if fails to the source type (possible unsafe conversion)
        DataType<?> coercedType = coerceTypeBase(fromType, toType.getTypeSignature().getBase());
        if (coercedType != null) {
            return compatibility(coercedType, toType);
        }

        coercedType = coerceTypeBase(toType, fromType.getTypeSignature().getBase());
        if (coercedType != null) {
            TypeCompatibility typeCompatibility = compatibility(fromType, coercedType);
            if (!typeCompatibility.isCompatible()) {
                return incompatible();
            }
            return compatible(typeCompatibility.getCommonSuperType(), false);
        }

        return incompatible();
    }

    @Nullable
    private static DataType<?> convertTypeByPrecedence(DataType<?> arg1, DataType<?> arg2) {
        final DataType<?> higherPrecedenceArg;
        final DataType<?> lowerPrecedenceArg;
        if (arg1.precedes(arg2)) {
            higherPrecedenceArg = arg1;
            lowerPrecedenceArg = arg2;
        } else {
            higherPrecedenceArg = arg2;
            lowerPrecedenceArg = arg1;
        }

        final boolean lowerPrecedenceCastable = lowerPrecedenceArg.isConvertableTo(higherPrecedenceArg);
        final boolean higherPrecedenceCastable = higherPrecedenceArg.isConvertableTo(lowerPrecedenceArg);

        if (lowerPrecedenceCastable) {
            return higherPrecedenceArg;
        } else if (higherPrecedenceCastable) {
            return lowerPrecedenceArg;
        }

        return null;
    }

    @Nullable
    private static DataType<?> coerceTypeBase(DataType<?> sourceType, String resultTypeBase) {
        DataType<?> resultType = parseTypeSignature(resultTypeBase).createType();
        if (resultType.equals(sourceType)) {
            return sourceType;
        }
        return convertTypeByPrecedence(sourceType, resultType);
    }

    private static boolean isCovariantParametrizedType(DataType<?> type) {
        // if we ever introduce contravariant, this function should be changed to return an enumeration: INVARIANT, COVARIANT, CONTRAVARIANT
        return type instanceof ObjectType || type instanceof ArrayType;
    }

    private static TypeCompatibility typeCompatibilityForCovariantParametrizedType(DataType<?> fromType, DataType<?> toType) {
        ArrayList<TypeSignature> commonParameterTypes = new ArrayList<>();
        List<DataType<?>> fromTypeParameters = fromType.getTypeParameters();
        List<DataType<?>> toTypeParameters = toType.getTypeParameters();

        if (fromTypeParameters.size() != toTypeParameters.size()) {
            return incompatible();
        }

        boolean coercible = true;
        for (int i = 0; i < fromTypeParameters.size(); i++) {
            TypeCompatibility compatibility = compatibility(fromTypeParameters.get(i), toTypeParameters.get(i));
            if (!compatibility.isCompatible()) {
                return incompatible();
            }
            coercible &= compatibility.isCoercible();
            commonParameterTypes.add(compatibility.getCommonSuperType().getTypeSignature());
        }
        String typeBase = fromType.getTypeSignature().getBase();
        return compatible(
            new TypeSignature(typeBase, Collections.unmodifiableList(commonParameterTypes)).createType(),
            coercible);
    }

    @Nullable
    private final DataType<?> commonSuperType;
    private final boolean coercible;

    private TypeCompatibility(DataType<?> commonSuperType, boolean coercible) {
        this.commonSuperType = commonSuperType;
        this.coercible = coercible;
    }

    public boolean isCompatible() {
        return commonSuperType != null;
    }

    public DataType<?> getCommonSuperType() {
        if (commonSuperType == null) {
            throw new IllegalStateException("Types are not compatible");
        }
        return commonSuperType;
    }

    public boolean isCoercible() {
        return coercible;
    }
}

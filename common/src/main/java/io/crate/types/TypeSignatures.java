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

public final class TypeSignatures {

    /**
     * Gets the type with the specified signature.
     */
    public static DataType<?> getType(TypeSignature signature) {
        String base = signature.getBase();
        List<TypeSignatureParameter> parameters = signature.getParameters();
        if (base.equalsIgnoreCase(ArrayType.NAME)) {
            if (parameters.size() == 0) {
                return new ArrayType<>(UndefinedType.INSTANCE);
            }
            DataType<?> innerType = getType(parameters.get(0).getTypeSignature());
            return new ArrayType<>(innerType);
        }
        if (base.equalsIgnoreCase(ObjectType.NAME)) {
            var builder = ObjectType.builder();
            for (int i = 0; i < parameters.size() - 1;) {
                var valTypeSignature = parameters.get(i + 1);
                builder.setInnerType(String.valueOf(i), getType(valTypeSignature.getTypeSignature()));
                i += 2;
            }
            return builder.build();
        }
        return DataTypes.ofName(signature.getBase());
    }

    @Nullable
    public static DataType<?> getCommonSuperType(DataType<?> firstType, DataType<?> secondType) {
        TypeCompatibility compatibility = compatibility(firstType, secondType);
        if (!compatibility.isCompatible()) {
            return null;
        }
        return compatibility.getCommonSuperType();
    }

    public static boolean canCoerce(DataType<?> fromType, DataType<?> toType) {
        return fromType.isConvertableTo(toType);
    }

    private static TypeCompatibility compatibility(DataType<?> fromType, DataType<?> toType) {
        if (fromType.equals(toType)) {
            return TypeCompatibility.compatible(toType, true);
        }

        if (fromType.equals(UndefinedType.INSTANCE)) {
            return TypeCompatibility.compatible(toType, true);
        }

        if (toType.equals(UndefinedType.INSTANCE)) {
            return TypeCompatibility.compatible(fromType, false);
        }

        // If given types share the same base, e.g. arrays, parameter types must be compatible.
        String fromTypeBaseName = fromType.getTypeSignature().getBase();
        String toTypeBaseName = toType.getTypeSignature().getBase();
        if (fromTypeBaseName.equals(toTypeBaseName)) {
            if (isCovariantParametrizedType(fromType)) {
                return typeCompatibilityForCovariantParametrizedType(fromType, toType);
            }
            return TypeCompatibility.compatible(fromType, false);
        }

        // Use possible common super type (safe conversion)
        DataType<?> commonSuperType = convertTypeByPrecedence(fromType, toType);
        if (commonSuperType != null) {
            return TypeCompatibility.compatible(commonSuperType, commonSuperType.equals(toType));
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
                return TypeCompatibility.incompatible();
            }
            return TypeCompatibility.compatible(typeCompatibility.getCommonSuperType(), false);
        }

        return TypeCompatibility.incompatible();
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
        DataType<?> resultType = getType(parseTypeSignature(resultTypeBase));
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
        ArrayList<TypeSignatureParameter> commonParameterTypes = new ArrayList<>();
        List<DataType<?>> fromTypeParameters = fromType.getTypeParameters();
        List<DataType<?>> toTypeParameters = toType.getTypeParameters();

        if (fromTypeParameters.size() != toTypeParameters.size()) {
            return TypeCompatibility.incompatible();
        }

        boolean coercible = true;
        for (int i = 0; i < fromTypeParameters.size(); i++) {
            TypeCompatibility compatibility = compatibility(fromTypeParameters.get(i), toTypeParameters.get(i));
            if (!compatibility.isCompatible()) {
                return TypeCompatibility.incompatible();
            }
            coercible &= compatibility.isCoercible();
            commonParameterTypes.add(TypeSignatureParameter.of(compatibility.getCommonSuperType().getTypeSignature()));
        }
        String typeBase = fromType.getTypeSignature().getBase();
        return TypeCompatibility.compatible(
            getType(new TypeSignature(typeBase, Collections.unmodifiableList(commonParameterTypes))),
            coercible);
    }

    public static class TypeCompatibility {
        @Nullable
        private final DataType<?> commonSuperType;
        private final boolean coercible;

        private TypeCompatibility(DataType<?> commonSuperType, boolean coercible) {
            this.commonSuperType = commonSuperType;
            this.coercible = coercible;
        }

        private static TypeCompatibility compatible(DataType<?> commonSuperType, boolean coercible) {
            return new TypeCompatibility(commonSuperType, coercible);
        }

        private static TypeCompatibility incompatible() {
            return new TypeCompatibility(null, false);
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
}

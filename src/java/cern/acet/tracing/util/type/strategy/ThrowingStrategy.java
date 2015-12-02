/**
 * Logalike - A stream based message processor
 * Copyright (c) 2015 European Organisation for Nuclear Research (CERN), All Rights Reserved.
 * This software is distributed under the terms of the GNU General Public Licence version 3 (GPL Version 3),
 * copied verbatim in the file “COPYLEFT”.
 * In applying this licence, CERN does not waive the privileges and immunities granted to it by virtue
 * of its status as an Intergovernmental Organization or submit itself to any jurisdiction. 
 * 
 * Authors: Gergő Horányi <ghoranyi> and Jens Egholm Pedersen <jegp>
 */

package cern.acet.tracing.util.type.strategy;

import java.util.Optional;

import cern.acet.tracing.util.Either;
import cern.acet.tracing.util.type.TypeConstraint;
import cern.acet.tracing.util.type.TypeStrategy;

/**
 * A {@link TypeStrategy} that throws a {@link UnsupportedOperationException} when an object is attempted stored under a
 * key without a type or a {@link IllegalArgumentException} if the inserted object has a different type than expected.
 */
public enum ThrowingStrategy implements TypeStrategy {

    INSTANCE;

    private static final String TYPE_MISSING_FORMAT = "Failed to insert value %s under key %s: No type mapping found.";
    private static final String TYPE_MISMATCH_FORMAT = "Type mismatch when inserting value %s with type %s under the key %s with required type %s";

    @Override
    public Either<String, Object> onMissingType(String key, Object value) {
        throw new UnsupportedOperationException(String.format(TYPE_MISSING_FORMAT, value, key));
    }

    @Override
    public Optional<String> onFailedCast(String key, Object value, TypeConstraint<?> constraint) {
        throw new IllegalArgumentException(String.format(TYPE_MISMATCH_FORMAT, value, value.getClass().getName(), key,
                constraint));
    }

}

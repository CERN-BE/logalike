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

package cern.acet.tracing.util.type;

import java.util.Map;
import java.util.Optional;

import cern.acet.tracing.util.Either;

/**
 * A {@link TypeStrategy} specifies what to do when attempting to store an object in a {@link TypedMap} where
 * <ol>
 * <li>The type information is missing ({@link #onMissingType(String, Object)}) or</li>
 * <li>The object cannot be cast to the expected type ({@link #onFailedCast(Map, String, Object, TypeConstraint)})</li>
 * </ol>
 *
 * @author jepeders
 */
public interface TypeStrategy {

    /**
     * Called whenever the {@link TypedMap} cannot find a type associated with the given field.
     * 
     * @param key The key the value was attempted inserted into.
     * @param value The value of the key-value pair.
     * @return {@link Either} an error or an object to insert in the place of the key.
     */
    Either<String, Object> onMissingType(String key, Object value);

    /**
     * Called whenever a cast cannot be completed, because the value does not live up to the {@link TypeConstraint}.
     * 
     * @param key The key the value was attempted inserted into.
     * @param value The value of the key-value pair.
     * @param constraint The constraint values under the key should comply to.
     * @return An optional error message.
     */
    Optional<String> onFailedCast(String key, Object value, TypeConstraint<?> constraint);

}
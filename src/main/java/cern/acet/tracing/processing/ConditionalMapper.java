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

package cern.acet.tracing.processing;

import java.util.function.Predicate;
import java.util.function.UnaryOperator;

import cern.acet.tracing.Message;

/**
 * A filter which is capable of modifying messages if they pass a predicate. This is useful for conditionally changing
 * the content of a {@link Message}.
 *
 * @param <T> The type of {@link Message} which can be subtyped for additional functionality.
 * @author jepeders
 */
public class ConditionalMapper<T extends Message<T>> implements MessageMapper<T> {

    private final UnaryOperator<T> mappingAction;
    private final Predicate<T> predicate;

    /**
     * Creates an {@link ConditionalMapper} which applies the given action on messages that fit the predicate.
     *
     * @param predicate The predicate to test if the action should be applied.
     * @param mappingAction The action to apply to incoming messages if the predicate evaluates to <code>true</code>.
     */
    public ConditionalMapper(Predicate<T> predicate, UnaryOperator<T> mappingAction) {
        this.predicate = predicate;
        this.mappingAction = mappingAction;
    }

    @Override
    public T apply(T message) {
        return predicate.test(message) ? mappingAction.apply(message) : message;
    }

}

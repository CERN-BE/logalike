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

import cern.acet.tracing.Message;

/**
 * A filter which can decide to keep or discard {@link Message}s based on whether the message lives up to some
 * condition(s) (predicate). As a predicate it can be chained by {@link #or(Predicate)}'ing or {@link #and(Predicate)} 
 * 'ing with other filters, giving a longer chain of filtering conditions.
 * 
 * @param <MessageType> The type of {@link Message} which can be subtyped to extend the functionality.
 * @author ghoranyi, jepeders
 */
@FunctionalInterface
public interface Filter<MessageType extends Message<MessageType>> extends Predicate<MessageType> {

    /**
     * Tests if the incoming {@link Message} passes this filter. If it does (true), the message is let through the
     * filter, if not (false), the message is removed.
     * 
     * @param message The message to test against this predicate.
     * @return True if the message passes the conditions of the filter, false otherwise.
     */
    @Override
    boolean test(MessageType message);

    /**
     * Converts the given {@link Predicate} to a {@link Filter}.
     * 
     * @param predicate The {@link Predicate} to convert.
     * @return A {@link Filter} with the same functionality as the {@link Predicate}.
     */
    static <MessageType extends Message<MessageType>> Filter<MessageType> ofPredicate(Predicate<MessageType> predicate) {
        return predicate::test;
    }
}

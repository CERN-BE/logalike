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
import java.util.regex.Pattern;

import cern.acet.tracing.Message;

/**
 * A filter that works as a predicate for {@link Message}s, checking if the message fulfills the predicate or not. As a
 * predicate it can be chained by {@link #or(Predicate)}'ing or {@link #and(Predicate)}'ing with other filters, giving a
 * longer chain of filtering conditions.
 *
 * @param <MessageType> The type of {@link Message} to filter. Can be extended for additional functionality.
 * @author jepeders
 */
@FunctionalInterface
public interface FilterPredicate<MessageType extends Message<MessageType>> extends Predicate<MessageType> {

    /**
     * Converts a regular {@link Predicate} that tests {@link Message}s into a {@link FilterPredicate}.
     *
     * @param predicate The {@link Predicate} to convert.
     * @return A {@link FilterPredicate}.
     */
    static <MessageType extends Message<MessageType>> FilterPredicate<MessageType> ofPredicate(
            Predicate<MessageType> predicate) {
        return predicate::test;
    }

    /**
     * Creates a {@link Filter} which bases its filtering on a regex pattern. If the field is not present in the
     * messages, we default to <code>true</code>.
     *
     * @param field The field to examine.
     * @param regex The regex the value of the field must match.
     * @param <MessageType> The type of {@link Message} to create a {@link FilterPredicate} over.
     * @return A {@link Filter} instance.
     */
    static <MessageType extends Message<MessageType>> FilterPredicate<MessageType> ofRegex(String field, String regex) {
        return ofRegex(field, regex, true);
    }

    /**
     * Creates a {@link Filter} which bases its filtering on a regex pattern. If the field is not present in the
     * messages, we default to the given boolean value in the {@code defaultValue} parameter.
     *
     * @param field The field to examine.
     * @param regex The regex the value of the field must match.
     * @param defaultValue The default value if the {@code field} parameter could not be found.
     * @param <MessageType> The type of {@link Message} to create a {@link FilterPredicate} over.
     * @return A {@link Filter} instance.
     */
    static <MessageType extends Message<MessageType>> FilterPredicate<MessageType> ofRegex(String field, String regex,
            boolean defaultValue) {
        final Pattern pattern = Pattern.compile(regex);
        return (message) -> message.getOptionalAs(field, String.class).map(data -> pattern.matcher(data).find())
                .orElse(defaultValue);

    }

    /**
     * Creates a {@link Filter} which bases its filtering on a message <b>not</b> matching the given regex pattern. If
     * the field is not present in the given field, we default to <code>true</code>.
     *
     * @param field The field to examine.
     * @param regex The regex the value of the field must <b>not</b> match.
     * @param <MessageType> The type of {@link Message} to create a {@link FilterPredicate} over.
     * @return A {@link Filter} instance.
     */
    static <MessageType extends Message<MessageType>> FilterPredicate<MessageType> ofRegexNot(String field, String regex) {
        return ofRegexNot(field, regex, true);
    }

    /**
     * Creates a {@link Filter} which bases its filtering on a message <b>not</b> matching the given regex pattern. If
     * the field is not present in the given field, we default to {@code defaultValue}.
     *
     * @param field The field to examine.
     * @param regex The regex the value of the field must <b>not</b> match.
     * @param defaultValue The default value to use if the field was not defined.
     * @param <MessageType> The type of {@link Message} to create a {@link FilterPredicate} over.
     * @return A {@link Filter} instance.
     */
    static <MessageType extends Message<MessageType>> FilterPredicate<MessageType> ofRegexNot(String field,
            String regex, boolean defaultValue) {
        final Pattern pattern = Pattern.compile(regex);
        return (message) -> {
            return message.getOptionalAs(field, String.class).map(data -> !pattern.matcher(data).find())
                    .orElse(defaultValue);
        };
    }

}

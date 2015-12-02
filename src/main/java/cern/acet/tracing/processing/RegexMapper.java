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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import cern.acet.tracing.Message;

/**
 * A {@link ConditionalMapper} which only executes an operation on {@link Message}s that fulfill a regex either
 * partially ({@link #ofFind(String, String, UnaryOperator)}) or fully ({@link #ofMatch(String, String, UnaryOperator)}
 * ).
 * 
 * @author jepeders
 * @param <T> The type of {@link Message} which can be subtyped for additional functionality.
 */
public class RegexMapper<T extends Message<T>> extends ConditionalMapper<T> {

    private RegexMapper(Predicate<T> predicate, UnaryOperator<T> mappingAction) {
        super(predicate, mappingAction);
    }

    /**
     * Creates an {@link RegexMapper} which applies the given action on messages where the given field exists and
     * matches a regex pattern partially (see {@link Matcher#find()}.
     *
     * @param field The field name to examine in {@link Message}s.
     * @param regex The regex to match against the value of the field.
     * @param action The action to apply when the value of the field partially matches the regex.
     * @return A {@link RegexMapper} which can perform the given action if the field exists and a part of its value
     *         matches the regex.
     */
    public static <T extends Message<T>> RegexMapper<T> ofFind(String field, String regex, UnaryOperator<T> action) {
        final Predicate<String> regexPredicate = Pattern.compile(regex).asPredicate();
        return new RegexMapper<T>(message -> message.getOptionalAs(field, String.class)
                .map(value -> regexPredicate.test(value)).orElse(false), action);
    }

    /**
     * Creates an {@link RegexMapper} which applies the given action on messages where the given field exists and
     * matches a regex pattern fully (see {@link Matcher#matches()}.
     *
     * @param field The field name to examine in {@link Message}s.
     * @param regex The regex to match against the value of the field.
     * @param action The action to apply when the value of the field partially matches the regex.
     * @return A {@link RegexMapper} which can perform the given action if the field exists and its value fully matches
     *         the regex.
     */
    public static <T extends Message<T>> RegexMapper<T> ofMatch(String field, String regex, UnaryOperator<T> action) {
        final Predicate<String> regexPredicate = string -> Pattern.compile(regex).matcher(string).matches();
        return new RegexMapper<T>(message -> message.getOptionalAs(field, String.class)
                .map(value -> regexPredicate.test(value)).orElse(false), action);
    }
}

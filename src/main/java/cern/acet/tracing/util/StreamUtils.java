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

package cern.acet.tracing.util;

import java.util.function.Predicate;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * A utility class for Java 8 {@link Stream}s. Heavily inspired by <a href="https://github.com/poetix/protonpack"
 * title="Protonpack on GitHub">Protonpack</a>.
 * 
 * @author jepeders
 */
public class StreamUtils {

    /**
     * Construct a stream which takes values from the source stream for as long as they meet the supplied condition, and
     * stops as soon as a value is encountered which does not meet the condition.
     * 
     * @param source The source stream.
     * @param condition The condition to apply to elements of the source stream.
     * @param <T> The type over which the stream streams.
     * @return A condition-bounded stream.
     */
    public static <T> Stream<T> takeWhile(Stream<T> source, Predicate<T> condition) {
        return StreamSupport.stream(TakeWhileSpliterator.over(source.spliterator(), condition), false);
    }

    /**
     * Construct a stream which takes values from the source stream until one of them meets the supplied condition, and
     * then stops.
     * 
     * @param source The source stream.
     * @param condition The condition to apply to elements of the source stream.
     * @param <T> The type over which the stream streams.
     * @return A condition-bounded stream.
     */
    public static <T> Stream<T> takeUntil(Stream<T> source, Predicate<T> condition) {
        return takeWhile(source, condition.negate());
    }

}

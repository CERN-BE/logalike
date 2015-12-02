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

import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * An immutable class that contains two values.
 * 
 * @author jepeders
 * @param <A> The type of the first value {@link #first};
 * @param <B> The type of the second value {@link #second};
 */
public class Tuple<A, B> {

    private final A first;
    private final B second;

    /**
     * Creates a {@link Tuple} with two values.
     * 
     * @param first The first value to store.
     * @param second The second value to store.
     */
    public Tuple(A first, B second) {
        this.first = first;
        this.second = second;
    }

    /**
     * Folds the first and second values of this tuple by applying the given functions on them and returning a new
     * {@link Tuple} with the results.
     * 
     * @param firstFunction The function to apply on the first value.
     * @param secondFunction The function to apply on the second value.
     * @return A new {@link Tuple} with the result of the first function applied to the first value and the second
     *         function applied to the second value.
     */
    public <T, V> Tuple<T, V> fold(Function<A, T> firstFunction, Function<B, V> secondFunction) {
        return Tuple.of(firstFunction.apply(first), secondFunction.apply(second));
    }

    /**
     * @return The first value.
     */
    public A getFirst() {
        return first;
    }

    /**
     * @return The second value.
     */
    public B getSecond() {
        return second;
    }

    /**
     * Maps the values in the {@link Tuple} with the given function, returning a new tuple with the mapped value.
     * 
     * @param function The function to apply to both the {@link #first} and {@link #second} value.
     * @return A new {@link Tuple} with the mapped values, resulting from applying the function <code>f</code> to this
     *         {@link Tuple}.
     */
    public <T, V> Tuple<T, V> map(BiFunction<A, B, Tuple<T, V>> function) {
        return function.apply(first, second);
    }

    /**
     * Maps the values in the {@link Tuple} with the given function, returning a new tuple with the mapped value.
     * 
     * @param function The function to apply to both the {@link #first} and {@link #second} value.
     * @return A new {@link Tuple} with the mapped values, resulting from applying the function <code>f</code> to this
     *         {@link Tuple}.
     */
    public <T, V> Tuple<T, V> map(Function<Tuple<A, B>, Tuple<T, V>> function) {
        return function.apply(this);
    }

    /**
     * Creates a {@link Tuple} with two values.
     * 
     * @param first The first value to store.
     * @param second The second value to store.
     * @return A {@link Tuple}.
     */
    public static <A, B> Tuple<A, B> of(A first, B second) {
        return new Tuple<A, B>(first, second);
    }

    @Override
    public String toString() {
        return "[" + first + ", " + second + "]";
    }

}

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

import java.util.NoSuchElementException;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Can either have a left or a right value. Idea stolen from Scala's Either classes.
 * "Convention dictates that Left is used for failure and Right is used for success".
 *
 * @see <a href="http://www.scala-lang.org/api/current/index.html#scala.util.Either">Either.scala</a>
 * @author jepeders
 * @param <L> The type of the {@link Left} value.
 * @param <R> The type of the {@link Right} value.
 */
public interface Either<L, R> {

    /**
     * Implementation note: At the time of writing a bug in eclipse prevents me from overloading the static 'left' and
     * 'right' method to simply 'of'. That should be cleaner for the implementor.
     */

    /**
     * Folds over the either instance and returns a common type.
     *
     * @param left The function to apply if this is a {@link Left}.
     * @param right The function to apply if this is a {@link Right}.
     * @param <T> The type to return after applying the function.
     * @return The result of applying the function to either a {@link Left} or a {@link Right}.
     */
    <T> T fold(Function<L, T> left, Function<R, T> right);

    /**
     * Applies the given void function on either the {@link Left} or {@link Right} value.
     *
     * @param left The function to apply if this is a {@link Left}.
     * @param right The function to apply if this is a {@link Right}.
     */
    void foreach(Consumer<L> left, Consumer<R> right);

    /**
     * @return True if this {@link Either} is a {@link Left}, false if it is a {@link Right}.
     */
    boolean isLeft();

    /**
     * @return True if this {@link Either} is a {@link Right}, false if it is a {@link Left}.
     */
    boolean isRight();

    /**
     * Attempts to forcefully convert this {@link Either} to a {@link Left}.
     *
     * @return This {@link Either} as a {@link Left}.
     * @throws NoSuchElementException If this is a {@link Right} and not a {@link Left}.
     */
    L left() throws NoSuchElementException;

    /**
     * Attempts to forcefully convert this {@link Either} to a {@link Right}.
     *
     * @return This {@link Either} as a {@link Right}.
     * @throws NoSuchElementException If this is a {@link Left} and not a {@link Right}.
     */
    R right() throws NoSuchElementException;

    /**
     * Creates a {@link Left} class with a given value.
     *
     * @param value The value of the {@link Left} class.
     * @param <L> The type of the {@link Left} value.
     * @param <R> The type of the {@link Right} value.
     * @return An instance of {@link Left}.
     */
    static <L, R> Left<L, R> left(L value) {
        return new Left<L, R>(value);
    }

    /**
     * Creates a {@link Right} class with a given value.
     *
     * @param value The value of the {@link Right} class.
     * @param <L> The type of the {@link Left} value.
     * @param <R> The type of the {@link Right} value.
     * @return An instance of {@link Right}.
     */
    static <L, R> Right<L, R> right(R value) {
        return new Right<L, R>(value);
    }

    /**
     * A class representing a left value (typically failure) in an {@link Either} instance.
     *
     * @author jepeders
     * @param <L> The type of the {@link Left} value.
     * @param <R> The type of the {@link Right} value.
     */
    class Left<L, R> implements Either<L, R> {

        private final L value;

        /**
         * Creates a {@link Left} with the given value.
         *
         * @param value The value of the left type.
         */
        public Left(L value) {
            this.value = value;
        }

        @Override
        public <T> T fold(Function<L, T> left, Function<R, T> right) {
            return left.apply(value);
        }

        @Override
        public void foreach(Consumer<L> left, Consumer<R> right) {
            left.accept(value);
        }

        @Override
        public L left() throws NoSuchElementException {
            return value;
        }

        @Override
        public R right() throws NoSuchElementException {
            throw new NoSuchElementException("Cannot convert a left to a right");
        }

        @Override
        public boolean isLeft() {
            return true;
        }

        @Override
        public boolean isRight() {
            return false;
        }

    }

    /**
     * A class representing a right value (typically success) in an {@link Either} instance.
     *
     * @author jepeders
     * @param <L> The type of the {@link Left} value.
     * @param <R> The type of the {@link Right} value.
     */
    class Right<L, R> implements Either<L, R> {

        private final R value;

        /**
         * Creates a {@link Right} with the given value.
         *
         * @param value The value of the right type.
         */
        public Right(R value) {
            this.value = value;
        }

        @Override
        public <T> T fold(Function<L, T> left, Function<R, T> right) {
            return right.apply(value);
        }

        @Override
        public void foreach(Consumer<L> left, Consumer<R> right) {
            right.accept(value);
        }

        @Override
        public L left() throws NoSuchElementException {
            throw new NoSuchElementException("Cannot convert a left to a right");
        }

        @Override
        public R right() throws NoSuchElementException {
            return value;
        }

        @Override
        public boolean isLeft() {
            return false;
        }

        @Override
        public boolean isRight() {
            return true;
        }

    }

}

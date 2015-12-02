package cern.acet.tracing.util;

import java.io.IOException;

/**
 * A {@link java.util.function.Consumer} which can throw.
 *
 * @param <T> The input type to the consumer.
 * @param <E> The type of exception to throw.
 */
@FunctionalInterface
public interface ThrowingConsumer<T, E extends Exception> {

    /**
     * Calls the consumer with the given value.
     * @param value The input to the function.
     * @throws E An exception of type {@link E}.
     */
    public void accept(T value) throws E;

}

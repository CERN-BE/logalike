package cern.acet.tracing.util;

/**
 * A {@link java.util.function.Function} which can throw.
 *
 * @param <T> The input type to the function.
 * @param <R> The return type of the function.
 * @param <E> The type of exception to throw.
 */
@FunctionalInterface
public interface ThrowingFunction<T, R, E extends Exception> {

    /**
     * Calls the function with the given value.
     * @param value The input to the function.
     * @return The output of the function.
     * @throws E An exception of type {@link E}.
     */
    public R apply(T value) throws E;
}

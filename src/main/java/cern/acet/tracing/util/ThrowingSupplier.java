package cern.acet.tracing.util;

/**
 * A {@link java.util.function.Supplier} which can throw.
 *
 * @param <R> The return type of the function.
 * @param <E> The type of exception to throw.
 */
@FunctionalInterface
public interface ThrowingSupplier<R, E extends Exception> {

    /**
     * Calls the supplier.
     * @return An object of type {@link R}
     * @throws E An exception of type {@link E}.
     */
    public R get() throws E;

}

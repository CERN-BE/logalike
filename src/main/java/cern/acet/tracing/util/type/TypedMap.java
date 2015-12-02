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

package cern.acet.tracing.util.type;

import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import cern.acet.tracing.util.type.strategy.DropStrategy;

import com.google.common.collect.ImmutableMap;

/**
 * <p>
 * An immutable map where fields are type-constrained to a class (type) of objects. If insertions or gets fail, a
 * {@link TypeStrategy} determines what to do. Provided a {@link Map} over keys and the restricted types, this
 * {@link TypedMap} can ensure that key-value pairs have the expected type.
 * </p>
 * <p>
 * Upholding these promises means that <code>put</code> and <code>get</code> operations can fail. The
 * {@link TypeStrategy} is made to keep track of what happens if a field is not typed (has no mapping in the type-map)
 * or an inserted value has a wrong type. See {@link #put} for more information.
 * </p>
 *
 * @param <T> The sub-type (or implementation) of the {@link TypedMap}. This type is used to ensure that the return type
 *            of methods in the {@link TypedMap} class, has the same type as the implementation.
 * @author jepeders
 */
public abstract class TypedMap<T extends TypedMap<T>> {

    public static final String TYPE_ERROR_KEY = "_typemappingerror";

    private static final String ERROR_MESSAGE_SEPARATOR = "\n";
    private static final String FORMAT_STRING = "[%s, %s, %s]";

    private final ImmutableMap<String, TypeConstraint<?>> typeMap;
    private final TypeStrategy typeStrategy;
    private final Map<String, Object> objectMap;

    /**
     * Creates an empty {@link TypedMap} that uses the given {@link ImmutableMap} to confine keys to certain types. The
     * constructor uses the most strict {@link TypeStrategy}, {@link TypeStrategy#THROW}.
     *
     * @param typeMap The {@link Map} to control the types in each field of the map.
     */
    public TypedMap(Map<String, TypeConstraint<?>> typeMap) {
        this(typeMap, DropStrategy.INSTANCE);
    }

    /**
     * Creates an empty {@link TypedMap} that uses given {@link #typeMap} to enforce that values are of a certain type.
     *
     * @param typeMap A map that contains information about which type belongs to which field.
     * @param strategy The strategy to use when a value is being inserted into a field that hos no type yet.
     */
    public TypedMap(Map<String, TypeConstraint<?>> typeMap, TypeStrategy strategy) {
        this(new ConcurrentHashMap<>(), typeMap, strategy);
    }

    /**
     * Creates an {@link TypedMap} from an object map, {@link #typeMap} and {@link TypeStrategy}. This constructor DOES
     * NOT guarantee that the data in the object map complies to the types in the {@link #typeMap} .
     *
     * @param objectMap The object map containing the object types.
     * @param typeMap The map with type restrictions for fields.
     * @param strategy The {@link TypeStrategy} explaining what to do if a field without a type is attempted inserted.
     */
    protected TypedMap(Map<String, Object> objectMap, Map<String, TypeConstraint<?>> typeMap, TypeStrategy strategy) {
        this.objectMap = new ConcurrentHashMap<>(objectMap);
        this.typeStrategy = strategy;
        this.typeMap = ImmutableMap.copyOf(typeMap);
    }

    private synchronized Map<String, Object> addError(String errorMessage) {
        Object existingError = objectMap.get(TYPE_ERROR_KEY);
        if (existingError == null) {
            objectMap.put(TYPE_ERROR_KEY, errorMessage);
        } else {
            objectMap.put(TYPE_ERROR_KEY, existingError + ERROR_MESSAGE_SEPARATOR + errorMessage);
        }
        return objectMap;
    }

    /**
     * Examines if a key-value pair exists in this {@link TypedMap}.
     *
     * @param key The key to search for.
     * @return True if this {@link TypedMap} contains the given key, false otherwise.
     */
    public boolean containsKey(String key) {
        return toMap().containsKey(key);
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        } else if (other instanceof TypedMap) {
            TypedMap<?> that = (TypedMap<?>) other;
            return typeMap.equals(that.typeMap) && objectMap.equals(that.objectMap)
                    && typeStrategy.equals(that.typeStrategy);
        } else {
            return false;
        }
    }

    /**
     * Returns a value of a field as an {@link Object}.
     *
     * @param field The name of the field to return.
     * @return An {@link Object}.
     * @throws NoSuchElementException If no element with that name exists in the map.
     */
    public Object get(String field) throws NoSuchElementException {
        return getAs(field, Object.class);
    }

    /**
     * Tries to find and return the value of the given field as an object of a specific type.
     *
     * @param field The field whose value to find.
     * @param expectedType The type of the value to extract.
     * @return An element of the given type.
     * @throws IllegalArgumentException If the expected type did not match what was stored in this map.
     * @throws NoSuchElementException If no elements exists with the given field-key.
     */
    public <R> R getAs(String field, Class<R> expectedType) throws IllegalArgumentException, NoSuchElementException {
        final Object object = Optional.ofNullable(objectMap.get(field)).orElseThrow(
                () -> new NoSuchElementException(field));
        final Class<?> objectClass = object.getClass();

        if (!expectedType.isAssignableFrom(objectClass)) {
            throw new IllegalArgumentException(String.format("Cannot extract a field of type %s as a type %s",
                    objectClass, expectedType));
        }

        return expectedType.cast(object);
    }

    /**
     * Returns a value of a field as a string.
     *
     * @param field The name of the field to return.
     * @return A String.
     * @throws NoSuchElementException If no element with that name exists in the map.
     * @throws IllegalArgumentException If the type of the element in that field is not a String.
     */
    public String getAsString(String field) throws IllegalArgumentException, NoSuchElementException {
        return getAs(field, String.class);
    }

    /**
     * Attempts to find an object stored under the given key.
     *
     * @param field The key to find the value under.
     * @param expectedType The expected type of the value.
     * @return The value under the key with the expected type or {@link Optional#empty()} if the key could not be found
     *         or the type was not as expected.
     */
    public Optional<Object> getOptional(String field) {
        return getOptionalAs(field, Object.class);
    }

    /**
     * Attempts to find a value stored under the given key and cast it to the expected type.
     *
     * @param field The field to find the value under.
     * @param expectedType The expected type of the value.
     * @return The value under the key with the expected type or {@link Optional#empty()} if the key could not be found
     *         or the type was not as expected.
     */
    public <R> Optional<R> getOptionalAs(String field, Class<R> expectedType) {
        final Object value = objectMap.get(field);
        if (value != null && expectedType.isAssignableFrom(value.getClass())) {
            return Optional.of(expectedType.cast(value));
        } else {
            return Optional.empty();
        }
    }

    /**
     * @return The implementation of the {@link TypedMap}, used when a method needs to return the correct implementation
     *         type.
     */
    protected abstract T getThis();

    /**
     * @return An {@link ImmutableMap} containing the type constraints applied to this {@link TypedMap}.
     */
    public ImmutableMap<String, TypeConstraint<?>> getTypeMap() {
        return typeMap;
    }

    /**
     * @return The {@link TypeStrategy} of this {@link TypedMap}.
     */
    public TypeStrategy getTypeStrategy() {
        return typeStrategy;
    }

    @Override
    public int hashCode() {
        return Objects.hash(typeMap, objectMap, typeStrategy);
    }

    /**
     * <p>
     * Puts a field (<code>key</code>) with the given <code>value</code> into this map and returns the same map. If the
     * <code>key</code> does not exist in the type mapping or the <code>value</code> has a different type than the
     * type-mapping prescribes, the {@link TypeStrategy} of the {@link TypedMap} decides what happens.
     * </p>
     * <h4>If the type information is missing</h4>
     * <p>
     * If the type does not exist and the {@link TypeStrategy} is set to either {@link TypeStrategy#ACCEPT} or
     * {@link TypeStrategy#STRINGIFY}, then the field is inserted as an {@link Object} or {@link String} respectively.
     * If the {@link TypeStrategy} is set to {@link TypeStrategy#DROP}, the value is dropped, but an error message is
     * inserted in the '{@value #TYPE_ERROR_KEY}' field. And if the strategy is {@link TypeStrategy#THROW} an
     * {@link IllegalArgumentException} is thrown.
     * </p>
     * <h4>If the type is malformed</h4>
     * <p>
     * If the type information exists, but the value does not uphold it (it is not a subtype) the
     * {@link TypeStrategy#THROW} throws an {@link UnsupportedOperationException}, while other values (
     * {@link TypeStrategy#ACCEPT}, {@link TypeStrategy#DROP}, and {@link TypeStrategy#STRINGIFY}) will drop the value,
     * but insert an error in the '{@value #TYPE_ERROR_KEY}' field of the returned message.
     * </p>
     *
     * @param field The key to insert the value under.
     * @param value The value of the field.
     * @return If the type-mapping contains the field and the value lives up to the rules, the key-value pair is
     *         inserted and <code>this</this> is returned. If not, the {@link TypeStrategy} decides what to do
     *         (see above).
     * @throws IllegalArgumentException If the type information for the key is not found.
     * @throws UnsupportedOperationException If the type information exists and the <code>value</code> is not a sub-type
     *             of the expected class.
     */
    public T put(String field, Object value) throws IllegalArgumentException, UnsupportedOperationException {
        putImpl(field, value);
        return getThis();
    }

    private void putImpl(String field, Object value) throws IllegalArgumentException, UnsupportedOperationException {
        final TypeConstraint<?> expectedType = typeMap.get(field);
        if (expectedType == null) {
            typeStrategy.onMissingType(field, value).fold(this::addError, object -> objectMap.put(field, object));
        } else if (expectedType.canCast(value)) {
            objectMap.put(field, expectedType.cast(value));
        } else {
            typeStrategy.onFailedCast(field, value, expectedType).map(this::addError);
        }
    }

    /**
     * Attempts to insert all the values in the given map into this {@link TypedMap}. Depending on the
     * {@link TypeStrategy} this can fail in various ways. See {@link #put(String, Object)}.
     *
     * @param map The map of elements to insert.
     * @return This map with the given map inserted.
     * @throws IllegalArgumentException If the {@link TypeStrategy} is set to {@link TypeStrategy#THROW} and the type of
     *             one of the inserting objects does not conform to the expected type.
     * @throws UnsupportedOperationException If the {@link TypeStrategy} is set to {@link TypeStrategy#THROW} and no
     *             type mapping for the field could be found.
     * @see #put(String, Object)
     */
    public T put(Map<String, Object> map) throws IllegalArgumentException, UnsupportedOperationException {
        map.entrySet().forEach(entry -> putImpl(entry.getKey(), entry.getValue()));
        return getThis();
    }

    /**
     * Removes the given key from this {@link TypedMap}.
     *
     * @param key The key to remove.
     * @return The implementation of the {@link TypedMap} with the given key removed.
     */
    public T remove(String key) {
        objectMap.remove(key);
        return getThis();
    }

    /**
     * Removes all the given keys from this {@link TypedMap}.
     *
     * @param keys The keys to remove.
     * @return The implementation of the {@link TypedMap} with the given keys removed.
     */
    public T removeAll(Set<String> keys) {
        keys.forEach(objectMap::remove);
        return getThis();
    }

    /**
     * @return The number of key-value pairs stored in the {@link TypedMap}.
     */
    public int size() {
        return objectMap.size();
    }

    /**
     * Converts this {@link TypedMap} into an {@link Map} with its current key-value pairs. The returned map is a copy
     * of the actual data to avoid any mutations.
     *
     * @return A copy of the key-value pairs of this map.
     */
    public Map<String, Object> toMap() {
        return new HashMap<>(objectMap);
    }

    @Override
    public String toString() {
        return String.format(FORMAT_STRING, objectMap.toString(), typeMap.toString(), typeStrategy.toString());
    }

}

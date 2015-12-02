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

/**
 * A constraint that can be used to verify if an object has an expected type and cast any object to that type.
 * 
 * @author jepeders
 * @param <T> The type this {@link TypeConstraint} tries to uphold.
 */
public class TypeConstraint<T> {

    private final Class<T> classConstraint;

    /**
     * Creates a {@link TypeConstraint} over the given {@link Class}.
     * 
     * @param constraint The super-class of the {@link TypeConstraint}.
     */
    private TypeConstraint(Class<T> constraint) {
        this.classConstraint = constraint;
    }

    /**
     * Examines if the given object can be cast to a class, that meets this {@link TypeConstraint}.
     * 
     * @param object The object to cast.
     * @return True if the type of the object is the same or a sub-type to the constraint in this class.
     */
    public boolean canCast(Object object) {
        return classConstraint.isAssignableFrom(object.getClass());
    }

    /**
     * Casts the given object to the type of this {@link TypeConstraint}.
     * 
     * @param object The object to cast.
     * @return An instance of type <code>T</code>
     */
    public T cast(Object object) {
        return classConstraint.cast(object);
    }

    /**
     * @return The name of the type.
     * @see Class#getName()
     */
    public String getTypeName() {
        return classConstraint.getName();
    }

    /**
     * Tests whether the given class is equal to or a sub-type of the constraint of this {@link TypeConstraint}.
     * 
     * @param classToTest The class to verify if it is equal to or a sub-type of this {@link TypeConstraint}.
     * @return True if the given class is equal to or a sub-type of this {@link TypeConstraint}, false otherwise.
     */
    public boolean isSubclassOf(Class<?> classToTest) {
        return classConstraint.isAssignableFrom(classToTest);
    }

    /**
     * Creates a {@link TypeConstraint} based on the given {@link Class}.
     * 
     * @param classConstraint The {@link Class} describing the type-constraint.
     * @return A {@link TypeConstraint} which constrains types to be equal to or a sub-type of the given class.
     */
    public static <T> TypeConstraint<T> ofClass(Class<T> classConstraint) {
        return new TypeConstraint<T>(classConstraint);
    }

    /**
     * @return The inner type constraint as given in {@link #getTypeName()} surrounded by "TypeConstraint[]".
     */
    @Override
    public String toString() {
        return "TypeConstraint[" + getTypeName() + "]";
    }

}

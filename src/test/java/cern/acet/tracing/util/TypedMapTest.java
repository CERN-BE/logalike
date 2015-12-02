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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Map;
import java.util.NoSuchElementException;

import org.junit.Before;
import org.junit.Test;

import cern.acet.tracing.util.type.TypeConstraint;
import cern.acet.tracing.util.type.TypeStrategy;
import cern.acet.tracing.util.type.TypedMap;
import cern.acet.tracing.util.type.strategy.DropStrategy;
import cern.acet.tracing.util.type.strategy.ThrowingStrategy;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

public class TypedMapTest {

    private static final String OBJECT_KEY = "object";
    private static final String STRING_KEY = "string";
    private static final String INT_KEY = "integer";
    private static final ImmutableMap<String, TypeConstraint<?>> DEFAULT_TYPE_MAP = ImmutableMap
            .<String, TypeConstraint<?>> of(
            STRING_KEY, TypeConstraint.ofClass(String.class), INT_KEY, TypeConstraint.ofClass(Integer.class),
            OBJECT_KEY, TypeConstraint.ofClass(Object.class));

    private TypedMapImpl map;

    @Before
    public void setup() {
        map = new TypedMapImpl(DEFAULT_TYPE_MAP, ThrowingStrategy.INSTANCE);
    }

    @Test
    public void canTestIfAKeyIsContained() {
        assertTrue(map.put(STRING_KEY, "test").containsKey(STRING_KEY));
    }

    @Test
    public void canTestIfAKeyIsNotContained() {
        assertFalse(map.containsKey(STRING_KEY));
    }

    @Test(expected = NoSuchElementException.class)
    public void canFailToGetNonExistingValue() {
        map = new TypedMapImpl(DEFAULT_TYPE_MAP, DropStrategy.INSTANCE);
        map.getAs("fail", String.class);
    }

    @Test(expected = NoSuchElementException.class)
    public void canFailToGetNonExistingType() {
        map.getAs("fail", Integer.class);
    }

    @Test
    public void canFailToGetNonExistingOptionalValue() {
        map = new TypedMapImpl(DEFAULT_TYPE_MAP, DropStrategy.INSTANCE);
        assertFalse(map.getOptionalAs("fail", String.class).isPresent());
    }

    @Test
    public void canFailToGetNonExistingOptionalType() {
        assertFalse(map.getOptionalAs("fail", Integer.class).isPresent());
    }

    @Test
    public void canGetTypedValue() {
        String value = "hello world";
        map = map.put(STRING_KEY, value);
        assertEquals(value, map.getAs(STRING_KEY, String.class));
        assertEquals(value, map.getOptionalAs(STRING_KEY, String.class).get());
    }

    @Test
    public void canGetStringAsObject() {
        String value = "valueTest";
        map = map.put(STRING_KEY, value);
        assertEquals(value, map.getAs(STRING_KEY, Object.class));
    }

    @Test(expected = IllegalArgumentException.class)
    public void canNotGetObjectAsString() {
        Object value = new Object();
        map = map.put(OBJECT_KEY, value);
        assertEquals(value, map.getAs(OBJECT_KEY, String.class));
    }

    @Test
    public void isMutable() {
        String value = "hello world";
        map.put(STRING_KEY, value);
        assertTrue(map.getOptionalAs(STRING_KEY, String.class).isPresent());
    }

    /* Removals */

    @Test
    public void canRemoveKey() {
        assertEquals(map, map.put(STRING_KEY, "value").remove(STRING_KEY));
    }

    @Test
    public void canOnlyRemoveOneKey() {
        TypedMapImpl intMap = map.put(INT_KEY, 5);
        assertEquals(intMap, intMap.put(STRING_KEY, "value").remove(STRING_KEY));
    }

    @Test
    public void canRemoveKeys() {
        assertEquals(map, map.put(STRING_KEY, "value").put(INT_KEY, 3).removeAll(ImmutableSet.of(STRING_KEY, INT_KEY)));
    }

    @Test
    public void canRemoveKeyThatIsNotThere() {
        assertEquals(map, map.remove(STRING_KEY));
    }

    /* Insertions */

    @Test(expected = IllegalArgumentException.class)
    public void canFailToInsertWrongType() {
        map.put(STRING_KEY, 1);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void canFailToInsertNonExistingType() {
        map.put("fail", 1);
    }

    @Test
    public void canInsertTypedValue() {
        String value = "hello world";
        assertEquals(value, map.put(STRING_KEY, value).getAs(STRING_KEY, String.class));
    }

    @Test
    public void canInsertTypedValues() {
        String stringValue = "hello world";
        int intValue = 2;
        TypedMapImpl newMap = map.put(ImmutableMap.of(STRING_KEY, stringValue, INT_KEY, intValue));
        assertEquals(stringValue, newMap.getAs(STRING_KEY, String.class));
        assertEquals(Integer.valueOf(intValue), newMap.getAs(INT_KEY, Integer.class));
    }

    @Test
    public void canInsertSingleValuesInSerial() {
        assertEquals(2, map.put(STRING_KEY, "hi").put(INT_KEY, 3).size());
    }

    @Test
    public void canOverwriteTypedValue() {
        String value1 = "hello world";
        String value2 = "Hi, there";
        assertEquals(value1, map.put(STRING_KEY, value1).getAs(STRING_KEY, String.class));
        assertEquals(value2, map.put(STRING_KEY, value1).put(STRING_KEY, value2).getAs(STRING_KEY, String.class));
    }

    @Test
    public void canInsertSubType() {
        String key = "key";
        map = new TypedMapImpl(ImmutableMap.of(key, TypeConstraint.ofClass(Object.class)));
        map.put(key, 1);
    }

    @Test
    public void cannotInsertSuperType() {
        String key = "key";
        map = new TypedMapImpl(ImmutableMap.of(key, TypeConstraint.ofClass(Integer.class)));
        map.put(key, new Object());
        assertFalse(map.containsKey(key));
    }

    @Test(expected = IllegalArgumentException.class)
    public void cannotInsertSuperTypeWithThrowStrategy() {
        String key = "key";
        map = new TypedMapImpl(ImmutableMap.of(key, TypeConstraint.ofClass(Integer.class)), ThrowingStrategy.INSTANCE);
        map.put(key, new Object());
    }

    @Test
    public void canInsertMultipleValues() {
        String value = "hi";
        Map<String, Object> values1 = ImmutableMap.of(STRING_KEY, value);
        assertEquals(value, map.put(values1).getAs(STRING_KEY, String.class));
    }

    @Test
    public void canInsertMultipleValuesInSerial() {
        Map<String, Object> values1 = ImmutableMap.of(STRING_KEY, "hi");
        Map<String, Object> values2 = ImmutableMap.of(INT_KEY, 2);
        assertEquals(2, map.put(values1).put(values2).size());
    }

    private final static class TypedMapImpl extends TypedMap<TypedMapImpl> {

        public TypedMapImpl(Map<String, TypeConstraint<?>> typeMap) {
            super(typeMap);
        }

        public TypedMapImpl(Map<String, TypeConstraint<?>> typeMap, TypeStrategy strategy) {
            super(typeMap, strategy);
        }

        @Override
        protected TypedMapImpl getThis() {
            return this;
        }

    }

}

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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.function.Predicate;

import org.junit.Before;
import org.junit.Test;

import cern.acet.tracing.MessageImpl;

public class FilterPredicateTest {

    private static final String BODY_FIELD = "body";
    private static final String BODY_VALUE = "accepted";

    private MessageImpl message;

    @Before
    public void setup() {
        message = MessageImpl.ofUntyped().put(BODY_FIELD, BODY_VALUE + " other data");
    }

    @Test
    public void canFilterOnRegex() {
        Predicate<MessageImpl> filter = FilterPredicate.ofRegex(BODY_FIELD, BODY_VALUE);
        assertTrue(filter.test(message));
    }

    @Test
    public void canFilterOnRegexNegated() {
        Predicate<MessageImpl> filter = FilterPredicate.ofRegexNot(BODY_FIELD, BODY_VALUE);
        assertFalse(filter.test(message));
    }

    @Test
    public void canGetSameResultFromNotNegated() {
        Predicate<MessageImpl> filter = FilterPredicate.ofRegex(BODY_FIELD, BODY_VALUE);
        Predicate<MessageImpl> filterNot = FilterPredicate.ofRegexNot(BODY_FIELD, BODY_VALUE);
        assertEquals(filter.test(message), filterNot.negate().test(message));
    }

    @Test
    public void canDefaultToTrueIfFieldDoesNotExist() {
        Predicate<MessageImpl> filter = FilterPredicate.ofRegex("doesnotexist", BODY_VALUE);
        assertTrue(filter.test(message));
    }

    @Test
    public void canDefaultToGivenBooleanOnRegex() {
        Predicate<MessageImpl> filter = FilterPredicate.ofRegex("doesnotexist", BODY_VALUE, false);
        assertFalse(filter.test(message));
    }

    @Test
    public void canDefaultToTrueForNegatedFilterIfFieldDoesNotExist() {
        Predicate<MessageImpl> filter = FilterPredicate.ofRegexNot("doesnotexist", BODY_VALUE);
        assertTrue(filter.test(message));
    }

    @Test
    public void canDefaultToGivenBooleanOnRegexNegated() {
        Predicate<MessageImpl> filter = FilterPredicate.ofRegexNot("doesnotexist", BODY_VALUE, true);
        assertTrue(filter.test(message));
    }

}

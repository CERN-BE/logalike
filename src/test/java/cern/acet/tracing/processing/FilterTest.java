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

import static org.junit.Assert.assertTrue;

import java.util.function.Predicate;

import org.junit.Before;
import org.junit.Test;

import cern.acet.tracing.MessageImpl;

public class FilterTest {

    private static final Predicate<MessageImpl> PREDICATE_TRUE = m -> true;
    private static final Predicate<MessageImpl> PREDICATE_FALSE = m -> false;

    private Predicate<MessageImpl> filter;
    private MessageImpl message;

    @Before
    public void setup() {
        message = new MessageImpl();
    }

    @Test
    public void canFilterFromAPredicate() {
        filter = Filter.ofPredicate(PREDICATE_TRUE);
        assertTrue(filter.test(message));
    }

    @Test
    public void canFilterFromANegatedPredicate() {
        filter = Filter.ofPredicate(PREDICATE_FALSE).negate();
        assertTrue(filter.test(message));
    }

}

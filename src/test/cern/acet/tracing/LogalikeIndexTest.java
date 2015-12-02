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

package cern.acet.tracing;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.time.LocalDate;
import java.util.Calendar;

import org.junit.Test;

import cern.acet.tracing.output.elasticsearch.ElasticsearchIndex;

public class LogalikeIndexTest {

    private static final String FORMAT = "%tY.%tm.%td";
    private static final String CURRENT_DATE = String.format(FORMAT, Calendar.getInstance(), Calendar.getInstance(),
            Calendar.getInstance());
    private static final String INDEX_NAME = "testName";

    @Test
    public void canUseCorrectDate() throws Exception {
        assertTrue(ElasticsearchIndex.daily(INDEX_NAME).toString().endsWith(CURRENT_DATE));
    }

    @Test
    public void canFormatWithDay() {
        assertTrue(ElasticsearchIndex.daily(INDEX_NAME).toString().endsWith(String.format("%td", Calendar.getInstance())));
    }

    @Test
    public void canFormatWithMonth() throws Exception {
        assertTrue(ElasticsearchIndex.monthly(INDEX_NAME).toString().endsWith("01"));
    }

    @Test
    public void canFormatWithDate() throws Exception {
        String dateIndex = INDEX_NAME + ElasticsearchIndex.SEPARATOR + CURRENT_DATE;
        assertEquals(dateIndex, ElasticsearchIndex.daily(INDEX_NAME).toString(LocalDate.now()));
    }

    @Test(expected=IllegalArgumentException.class)
    public void canFailOnEmptyName() throws Exception {
        ElasticsearchIndex.daily("");
    }

}

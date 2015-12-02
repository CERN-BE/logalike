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

package cern.acet.tracing.output.elasticsearch;

import java.time.Period;
import java.time.format.DateTimeFormatter;

/**
 * An enum that describes with which interval an index is renewed and can convert that to a {@link DateTimeFormatter} to
 * format dates to strings.
 * 
 * @author jepeders
 */
public enum Frequency {

    /**
     * A frequency of one day.
     */
    DAILY(Period.ofDays(1), DateTimeFormatter.ofPattern("YYYY.MM.dd")),

    /**
     * A frequency of one month.
     */
    MONTHLY(Period.ofMonths(1), DateTimeFormatter.ofPattern("YYYY.MM.01")),

    /**
     * A frequency that never changes, indicating an interval without a date/time-stamp.
     */
    CONSTANT(Period.ZERO, DateTimeFormatter.ofPattern(""));

    private final DateTimeFormatter formatter;
    private final Period period;

    private Frequency(Period period, DateTimeFormatter formatter) {
        this.formatter = formatter;
        this.period = period;
    }

    /**
     * Retrieves a formatter that fits the frequency in the format of "YYYY.MM.dd".
     * 
     * @return A {@link DateTimeFormatter} instance
     */
    public DateTimeFormatter getFormatter() {
        return formatter;
    }

    /**
     * Retrieves this frequency as represented by a {@link Period}.
     * 
     * @return A Period.
     */
    public Period getPeriod() {
        return period;
    }

}
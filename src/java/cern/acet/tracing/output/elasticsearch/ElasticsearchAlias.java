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

import java.time.LocalDate;

import cern.acet.tracing.Message;

/**
 * An immutable alias that describes the destination of one or more {@link Message}s. The alias consists of a name and
 * an optional suffix time-stamp.
 *
 * @author jepeders
 */
public final class ElasticsearchAlias {

    public static final String SEPARATOR = "-";

    private final Frequency frequency;
    private final String aliasName;

    /**
     * Creates an alias with the given name, suffixed with a time-stamp that is updated with the given frequency.
     *
     * @param aliasName The prefix of the alias.
     * @param frequency The frequency with which to update the time-stamp.
     */
    public ElasticsearchAlias(String aliasName, Frequency frequency) {
        if (aliasName.isEmpty()) {
            throw new IllegalArgumentException("Alias name cannot be empty");
        }
        this.frequency = frequency;
        this.aliasName = aliasName;
    }

    /**
     * Creates a {@link ElasticsearchAlias} which changes daily.
     *
     * @param aliasName The name of the alias.
     * @return An alias which, when printed, will contain the current day.
     */
    public static ElasticsearchAlias daily(String aliasName) {
        return new ElasticsearchAlias(aliasName, Frequency.DAILY);
    }

    /**
     * Creates a {@link ElasticsearchAlias} which changes monthly.
     *
     * @param aliasName The name of the alias.
     * @return An alias which, when printed, will contain the current month.
     */
    public static ElasticsearchAlias monthly(String aliasName) {
        return new ElasticsearchAlias(aliasName, Frequency.MONTHLY);
    }

    /**
     * @return The stem of this alias.
     */
    public String getPrefix() {
        return aliasName;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        ElasticsearchAlias other = (ElasticsearchAlias) obj;
        if (aliasName == null) {
            if (other.aliasName != null) {
                return false;
            }
        } else if (!aliasName.equals(other.aliasName)) {
            return false;
        }
        if (frequency != other.frequency) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((aliasName == null) ? 0 : aliasName.hashCode());
        result = prime * result + ((frequency == null) ? 0 : frequency.hashCode());
        return result;
    }

    /**
     * Creates a {@link ElasticsearchAlias} without a time suffix.
     *
     * @param aliasName The name of the alias.
     * @return An alias which, when printed, will not contain any date/time suffix.
     */
    public static ElasticsearchAlias constant(String aliasName) {
        return new ElasticsearchAlias(aliasName, Frequency.CONSTANT);
    }

    /**
     * Creates a String containing the alias.
     *
     * @return A String with the name of the alias and a formatted time-stamp.
     */
    @Override
    public String toString() {
        if (frequency == Frequency.CONSTANT) {
            return aliasName;
        } else {
            return aliasName + SEPARATOR + frequency.getFormatter().format(LocalDate.now());
        }
    }

    /**
     * Creates a string containing the alias postfixed with a formatted date string of the given time.
     *
     * @param time The date to use when printing the date in the alias.
     * @return A String postfixed with a timestamp.
     */
    public String toString(LocalDate time) {
        if (frequency == Frequency.CONSTANT) {
            return aliasName;
        } else {
            return aliasName + SEPARATOR + frequency.getFormatter().format(time);
        }
    }

}

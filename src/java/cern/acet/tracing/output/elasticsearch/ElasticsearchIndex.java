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
 * An immutable index that describes the destination of one or more {@link Message}s. The index consists of a name, a
 * dash (-) and a suffix time-stamp so the format will turn out as: [name]-[timestamp].
 *
 * @author jepeders
 */
public class ElasticsearchIndex {

    public static final String SEPARATOR = "-";

    private final Frequency frequency;
    private final String indexName;

    /**
     * Creates an index with the given name, suffixed with a time-stamp that is updated with the given frequency.
     *
     * @param indexName The prefix of the index
     * @param frequency The frequency with which to update the time-stamp.
     */
    public ElasticsearchIndex(String indexName, Frequency frequency) {
        if (indexName.isEmpty()) {
            throw new IllegalArgumentException("Index name cannot be empty");
        }
        this.frequency = frequency;
        this.indexName = indexName;
    }

    /**
     * Creates an {@link ElasticsearchIndex} which is not associated with any date / {@link Frequency}, and thus never
     * changes name.
     * 
     * @param indexName The name of the index.
     * @return An index whose name will always be returned as {@link #indexName}.
     */
    public static ElasticsearchIndex constant(String indexName) {
        return new ElasticsearchIndex(indexName, Frequency.CONSTANT);
    }

    /**
     * Creates a {@link ElasticsearchIndex} which changes daily.
     *
     * @param indexName The name of the index.
     * @return An index which, when printed, will contain the current day.
     */
    public static ElasticsearchIndex daily(String indexName) {
        return new ElasticsearchIndex(indexName, Frequency.DAILY);
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
        ElasticsearchIndex other = (ElasticsearchIndex) obj;
        if (frequency != other.frequency) {
            return false;
        }
        if (indexName == null) {
            if (other.indexName != null) {
                return false;
            }
        } else if (!indexName.equals(other.indexName)) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        final int prime = 53;
        int result = 1;
        result = prime * result + ((frequency == null) ? 0 : frequency.hashCode());
        result = prime * result + ((indexName == null) ? 0 : indexName.hashCode());
        return result;
    }

    /**
     * @return The {@link Frequency} of how often the index changes over dates.
     */
    public Frequency getFrequency() {
        return frequency;
    }

    /**
     * Creates a {@link ElasticsearchIndex} which changes monthly
     *
     * @param indexName The name of the index.
     * @return An index which, when printed, will be named after the first day of the current month.
     */
    public static ElasticsearchIndex monthly(String indexName) {
        return new ElasticsearchIndex(indexName, Frequency.MONTHLY);
    }

    /**
     * Creates a String containing the index postfixed with a formatted date string.
     *
     * @return A String with the name of the index and a formatted time-stamp.
     */
    @Override
    public String toString() {
        return indexName + SEPARATOR + getFrequency().getFormatter().format(LocalDate.now());
    }

    /**
     * Creates a string containing the index postfixed with a formatted date string of the given time.
     *
     * @param time The date to use when printing the date in the index.
     * @return A String postfixed with a timestamp.
     */
    public String toString(LocalDate time) {
        return indexName + SEPARATOR + getFrequency().getFormatter().format(time);
    }

}

/**
 * Logalike - A stream based message processor
 * Copyright (c) 2015 European Organisation for Nuclear Research (CERN), All Rights Reserved.
 * This software is distributed under the terms of the GNU General Public Licence version 3 (GPL Version 3),
 * copied verbatim in the file “COPYLEFT”.
 * In applying this licence, CERN does not waive the privileges and immunities granted to it by virtue
 * of its status as an Intergovernmental Organization or submit itself to any jurisdiction.
 * <p>
 * Authors: Gergő Horányi <ghoranyi> and Jens Egholm Pedersen <jegp>
 */
package benchmark;

import java.time.Duration;
import java.time.Instant;

/**
 * A result of running a benchmark.
 */
public class BenchmarkResult {

    public final Instant endTime;
    public final int linesRead;
    public final Instant startTime;

    /**
     * Defines a result of running a benchmark.
     * @param startTime The point in time when the benchmark was started.
     * @param endTime The point in time when the benchmark was ended.
     * @param linesRead The number of lines read by the benchmark.
     */
    public BenchmarkResult(Instant startTime, Instant endTime, int linesRead) {
        this.startTime = startTime;
        this.endTime = endTime;
        this.linesRead = linesRead;
    }

    /**
     * The total {@link Duration} of the benchmark.
     * @return A {@link Duration}.
     */
    public Duration getDuration() {
        return Duration.between(startTime, endTime);
    }

}

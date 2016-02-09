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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.stream.Stream;

/**
 * A benchmark which warms up the reader before finally running the benchmark.
 */
public abstract class Benchmark {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Expected 2 arguments, but received " + args.length);
            System.err.println("Usage: benchmark linesToRead pathToLogstash");
            System.exit(1);
        }

        final int lines = Integer.parseInt(args[0]);
        final String pathToLogstash = args[1];

        if (lines < 1) {
            throw new IllegalArgumentException(String.format("Cannot benchmark less than 1 line (%d)!", lines));
        }

        System.out.println("Running Logalike 1/2");
        print("Logalike", new LogalikeBenchmark().run(lines));
        System.out.println("Running Logstash 2/2");
        print("Logstash", new LogstashBenchmark(pathToLogstash).run(lines));
        System.exit(0);
    }

    public static void print(String loggingSystem, Optional<BenchmarkResult> resultOption) {
        System.out.println(resultOption
                .map(result -> String.format("%s read %d lines in %dms",
                        loggingSystem, result.linesRead, result.getDuration().toMillis()))
                .orElse("Error completing benchmark"));
    }

    /**
     * Runs the benchmark with the given number of lines. The lines are generated at random and sent to the standard
     * input of the process, given by the {@link #getProcess()} implementation.
     *
     * @param lines The number of lines to load into the files.
     * @return A result if no error occurred.
     */
    public Optional<BenchmarkResult> run(int lines) throws Exception {
        final Process process = getProcess();
        try (final PrintWriter writer = new PrintWriter(process.getOutputStream());
             final BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
            final Stream<String> lineStream = Stream.generate(StringGenerator::generateString).limit(lines);
            return Optional.of(writeAndWait(lineStream, writer, reader, lines));
        } catch (IOException e) {
            System.err.println("Failed to run benchmark with error:");
            e.printStackTrace(System.err);
            return Optional.empty();
        } finally {
            process.destroy();
        }
    }

    /**
     * Creates a process that reads input into standard in and is expected to spit the same amount of lines out to
     * standard out. The process should be ready to process data by the time this function returns.
     */
    protected abstract Process getProcess() throws Exception;

    private BenchmarkResult writeAndWait(Stream<String> stream, PrintWriter writer, BufferedReader reader, int size)
            throws IOException {
        Executors.newSingleThreadExecutor().submit(() -> {
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            stream.forEach(line -> {
                writer.println(line);
                writer.flush();
            });
        });
        Instant start = null;
        int counter = 0;
        while (counter < size) {
            final String line = reader.readLine();
            if (line != null) {
                counter++;

                if (counter == 1) {
                    start = Instant.now();
                }
            }
        }
        Instant end = Instant.now();
        return new BenchmarkResult(start, end, size);
    }


}

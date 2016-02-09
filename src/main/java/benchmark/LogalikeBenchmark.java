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

import cern.acet.tracing.*;

import java.io.*;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

/**
 * A simple benchmark system for Logalike.
 */
public class LogalikeBenchmark extends Benchmark {

    public static void main(String[] args) {
        CloseableInput<MessageImpl> input = getInput(System.in);

        Output<MessageImpl> output = System.out::println;

        LogalikeImpl<MessageImpl> logalike = Logalike
                .<MessageImpl>builder()
                .setInput(input)
                .setOutput(output)
                .build();

        logalike.run();
    }

    private static CloseableInput<MessageImpl> getInput(InputStream input) {
        final BufferedReader reader = new BufferedReader(new InputStreamReader(input));
        return new CloseableInput<MessageImpl>() {
            @Override
            public Stream<MessageImpl> get() {
                return Stream.generate(() -> {
                    while (true) {
                        try {
                            final String line = reader.readLine();
                            if (line != null) {
                                return MessageImpl.ofUntyped().put("body", line);
                            }
                        } catch (IOException e) {
                            throw new RuntimeException(e);

                        }
                    }
                });
            }

            @Override
            public void close() throws IOException {
                reader.close();
            }
        };
    }

    @Override
    protected Process getProcess() throws Exception {
        return new ProcessBuilder()
                .command(getArguments())
                .start();
    }

    private List<String> getArguments() {
        final String javaHome = System.getProperty("java.home");
        final String javaBin = javaHome + File.separator + "bin" + File.separator + "java";
        final String classPath = getClass().getProtectionDomain().getCodeSource().getLocation().getFile();
        final String mainClass = "benchmark.LogalikeBenchmark";
        return Arrays.asList(javaBin, "-cp", classPath, mainClass);
    }

}

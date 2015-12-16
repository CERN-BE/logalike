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

package demo;

import cern.acet.tracing.Input;
import cern.acet.tracing.Logalike;
import cern.acet.tracing.MessageImpl;
import cern.acet.tracing.Output;
import cern.acet.tracing.input.file.FileInput;

import java.util.function.Function;
import java.util.stream.Stream;

/**
 * A demonstration on how to use Logsmart to read lines from a file and print them to std::out.
 * 
 * @author jepeders
 */
public class FileReadingDemonstration {

    public static void main(String[] args) {
        // Create a output that prints to system out
        Output<MessageImpl> output = System.out::println;

        // Create a converter that reads lines and spits out messages
        Function<String, MessageImpl> converter = line -> MessageImpl.ofUntyped().put("body", line);

        // Create the file input which tail the given file(s)
        FileInput<MessageImpl> input = FileInput.buildTailing(converter).addFiles("/tmp/logalike").build();

        // Create a Logalike instance that uses the input and output
        Logalike<MessageImpl> logsmart = Logalike.<MessageImpl> builder().setInput(input).setOutput(output).build();

        // Run it (preferably in a thread)
        logsmart.run();
    }
}

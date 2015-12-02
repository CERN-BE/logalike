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

import java.io.File;
import java.util.function.Function;

import cern.acet.tracing.Logalike;
import cern.acet.tracing.input.file.FileInput;
import cern.acet.tracing.output.ElasticsearchOutput;
import cern.acet.tracing.output.elasticsearch.ElasticsearchMessage;

/**
 * A demonstration on how to use Logsmart to read a file and send the contents to Elasticsearch.
 * 
 * @author jepeders
 */
public class LogalikeDemonstration {

    public static void main(String[] args) {
        // Create the output
        ElasticsearchOutput output = ElasticsearchOutput.builder().addHost("elasticsearchHost").build();

        // Write a converter that reads lines and converts them into messages
        Function<String, ElasticsearchMessage> converter = line -> output.createTypedMessage().put("body", line);

        // Create the input
        FileInput<ElasticsearchMessage> input = FileInput.<ElasticsearchMessage> buildTailing(converter)
                .addFile(new File("fileToReadFrom")).build();

        // Create a Logsmart instance that uses the input and output
        Logalike<ElasticsearchMessage> logsmart = Logalike.<ElasticsearchMessage> builder().setInput(input)
                .setOutput(output).build();

        // Run it (preferably in a thread)
        logsmart.run();
    }
}

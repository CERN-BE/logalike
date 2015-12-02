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

import cern.acet.tracing.output.ElasticsearchOutput;
import cern.acet.tracing.output.ElasticsearchOutput.Builder;
import cern.acet.tracing.output.elasticsearch.ElasticsearchIndex;
import cern.acet.tracing.output.elasticsearch.ElasticsearchMessage;
import cern.acet.tracing.output.elasticsearch.ElasticsearchTemplateMapping;
import cern.acet.tracing.util.type.strategy.AcceptStrategy;

/**
 * This is a demonstration of the use of the {@link ElasticsearchOutput} in three stages.
 * <ol>
 * <li>Create a connection to Elasticsearch</li>
 * <li>Send some data</li>
 * <li>Close the connection</li>
 * </ol>
 * 
 * @author jepeders
 */
public class ElasticsearchOutputDemonstration {

    public static void main(String[] args) {
        Builder builder = ElasticsearchOutput.builder();

        // //////////////////////////////
        // Stage 1 //////////////////////

        // Mandatory: The name of the host, on which an Elasticsearch instance is
        // expected to listen for TCP traffic on port 9300
        builder.addHost("hostname");

        // Non-mandatory: The name of the cluster. Defaults to "elasticsearch
        builder.setClusterName("anotherClusterName");

        // Non-mandatory: Sets the default index to direct documents, if no index has been specified
        builder.setDefaultIndex(ElasticsearchIndex.daily("logsmart"));

        // Non-mandatory: The name of the node as it will appear in the Elasticsearch cluster
        builder.setNodeName("nodeName");

        // Non-mandatory: Restrict the types of the message fields to a mapping, found from a template
        builder.setMapping(new ElasticsearchTemplateMapping("templateName", "mappingName"));

        // Non-mandatory: Defines what happens if insertions does not follow the type-restriction defined above
        builder.setTypeStrategy(AcceptStrategy.INSTANCE);

        // Builds the output
        ElasticsearchOutput output = builder.build();

        // //////////////////////////////
        // Stage 2 //////////////////////

        // Create a message with the type-restrictions defined above
        ElasticsearchMessage message = output.createTypedMessage();

        // Put some data into it
        message.put("key", "value");

        // Send it!
        output.accept(message);

        // //////////////////////////////
        // Stage 3 //////////////////////

        // Flush the output, so any remaining
        output.flush();

        // Close the output (recommended to use try-with-resource clauses)
        output.close();
    }
}

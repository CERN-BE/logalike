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

import org.elasticsearch.client.Client;

import cern.acet.tracing.Message;
import cern.acet.tracing.util.type.TypeConstraint;
import cern.acet.tracing.util.type.TypedMap;

import com.google.common.collect.ImmutableMap;

/**
 * A type mapping is a map that associated field-keys with value types, which can be used in {@link Message}s to
 * restrict insertions of values to the type (or a subtype) of the type specified in the type map.
 *
 * @author jepeders
 * @see TypedMap
 * @see Message
 */
public interface ElasticsearchTypeMapping {

    /**
     * Fetches a type-map that can be used to restrict the type of values inserted into a {@link Message}.
     * 
     * @param client The Elasticsearch {@link Client} to use when retrieving the type mapping.
     * @return An {@link ImmutableMap} of keys and their {@link TypeConstraint}s.
     */
    ImmutableMap<String, TypeConstraint<?>> getTypeMap(Client client);

}

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

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings.Builder;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

public class ClientBuilderTest {

    private static final String CLUSTER_NAME_KEY = "cluster.name";
    private static final String CLUSTER_HOSTS_KEY = "discovery.zen.ping.unicast.hosts";
    private static final List<String> CLUSTER_HOSTS = ImmutableList.of("host1", "host2");

    private Builder mockSettingsBuilder;
    private NodeBuilder mockNodeBuilder;
    private ClientBuilder builder;

    @Before
    public void setup() {
        mockSettingsBuilder = mock(Builder.class);
        mockNodeBuilder = mock(NodeBuilder.class);
        builder = new ClientBuilder(mockSettingsBuilder, mockNodeBuilder);
    }

    @Test
    public void canSetClusterName() {
        String name = "test";
        builder.setClusterName(name);
        verify(mockSettingsBuilder).put(CLUSTER_NAME_KEY, name);
    }

    @Test
    public void canSetClusterHostsInSettings() {
        final String expectedHosts = "host1,host2";
        builder.setHosts(CLUSTER_HOSTS);
        verify(mockSettingsBuilder).put(CLUSTER_HOSTS_KEY, expectedHosts);
    }

    @Test(expected = IllegalArgumentException.class)
    public void canFailWhenNoHostsAreDefined() {
        Client mockClient = mock(Client.class);
        Node mockNode = mock(Node.class);
        when(mockNodeBuilder.settings(mockSettingsBuilder)).thenReturn(mockNodeBuilder);
        when(mockNodeBuilder.node()).thenReturn(mockNode);
        when(mockNode.client()).thenReturn(mockClient);
        assertEquals(mockClient, builder.build());
    }

    @Test
    public void canCreateNode() {
        Client mockClient = mock(Client.class);
        Node mockNode = mock(Node.class);
        when(mockNodeBuilder.settings(mockSettingsBuilder)).thenReturn(mockNodeBuilder);
        when(mockNodeBuilder.node()).thenReturn(mockNode);
        when(mockNode.client()).thenReturn(mockClient);
        when(mockSettingsBuilder.get(CLUSTER_HOSTS_KEY)).thenReturn("nonNull");
        assertEquals(mockClient, builder.build());
    }

}

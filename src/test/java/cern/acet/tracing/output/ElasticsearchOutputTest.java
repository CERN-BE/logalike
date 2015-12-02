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

package cern.acet.tracing.output;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.ZonedDateTime;
import java.util.Optional;
import java.util.concurrent.CancellationException;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;

import cern.acet.tracing.output.elasticsearch.BulkConsumer;
import cern.acet.tracing.output.elasticsearch.ElasticsearchMessage;

public class ElasticsearchOutputTest {

    private Client mockClient;
    private ElasticsearchOutput.Builder builder;
    private BulkConsumer mockConsumer;
    private ElasticsearchOutput output;
    private ElasticsearchMessage mockMessage;
    private ElasticsearchOutput.Builder builderSpy;

    @Before
    public void setup() {
        mockClient = mock(Client.class);
        builder = ElasticsearchOutput.builder();
        builderSpy = spy(builder);
        mockConsumer = mock(BulkConsumer.class);
        mockMessage = mock(ElasticsearchMessage.class);

        doReturn(mockConsumer).when(builderSpy).getConsumer(Matchers.any());
        doReturn(mockClient).when(builderSpy).getClient();
        when(mockClient.settings()).thenReturn(Settings.EMPTY);

        output = new ElasticsearchOutput(builderSpy);
    }

    @Test
    public void canAcceptMessage() throws CancellationException {
        when(mockMessage.containsKey(Matchers.anyString())).thenReturn(true);
        output.accept(mockMessage);
        verify(mockConsumer).accept(mockMessage);
    }

    @Test
    public void canClose() throws Exception {
        output.close();
        verify(mockConsumer).close();
    }

    @Test
    public void canPreserveTimestamp() {
        ZonedDateTime time = ZonedDateTime.now();
        when(mockMessage.getTimestamp()).thenReturn(Optional.of(time));
        output.accept(mockMessage);
        verify(mockMessage, never()).putTimestamp(Matchers.any(ZonedDateTime.class));
    }

}

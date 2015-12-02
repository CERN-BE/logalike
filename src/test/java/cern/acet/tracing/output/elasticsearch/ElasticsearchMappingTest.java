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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Arrays;

import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesRequestBuilder;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesResponse;
import org.elasticsearch.client.IndicesAdminClient;
import org.junit.Before;
import org.junit.Test;

public class ElasticsearchMappingTest {

    private static final String MAPPING_NAME = "mapping";
    private static final String TEMPLATE_NAME = "template";
    private GetIndexTemplatesResponse mockResponse;
    private IndicesAdminClient mockClient;
    private ElasticsearchTemplateMapping mapping;

    @Before
    public void setup() {
        mockClient = mock(IndicesAdminClient.class);
        mockResponse = mock(GetIndexTemplatesResponse.class);
        mapping = new ElasticsearchTemplateMapping(TEMPLATE_NAME, MAPPING_NAME);

        GetIndexTemplatesRequestBuilder mockBuilder = mock(GetIndexTemplatesRequestBuilder.class);

        when(mockClient.prepareGetTemplates(TEMPLATE_NAME)).thenReturn(mockBuilder);
        when(mockBuilder.get()).thenReturn(mockResponse);
        when(mockResponse.getIndexTemplates()).thenReturn(Arrays.asList());
    }

    @Test
    public void canCallClient() {
        mapping.getTypeMap(mockClient, Clock.systemDefaultZone());
        verify(mockClient).prepareGetTemplates(TEMPLATE_NAME);
    }

    @Test
    public void canNotUpdateCacheIfWithinOneHour() {
        Clock startClock = Clock.fixed(Instant.now(), ZoneId.systemDefault());
        mapping.getTypeMap(mockClient, startClock);
        mapping.getTypeMap(mockClient, startClock);
        verify(mockClient, times(1)).prepareGetTemplates(TEMPLATE_NAME);
    }

    @Test
    public void canUpdateCacheIfAfterOneHour() {
        Clock startClock = Clock.fixed(Instant.now(), ZoneId.systemDefault());
        mapping.getTypeMap(mockClient, startClock);
        mapping.getTypeMap(mockClient, Clock.offset(startClock, Duration.ofHours(1).plusMillis(1)));
        verify(mockClient, times(2)).prepareGetTemplates(TEMPLATE_NAME);
    }

    @Test
    public void canOnlyUpdateOnceOnAsynchronousCalls() throws InterruptedException {
        Runnable update = () -> mapping.getTypeMap(mockClient, Clock.systemDefaultZone());
        Thread thread1 = new Thread(update);
        Thread thread2 = new Thread(update);

        thread1.start();
        thread2.start();
        thread1.join();
        thread2.join();

        verify(mockClient, times(1)).prepareGetTemplates(TEMPLATE_NAME);
    }

}

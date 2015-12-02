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
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Matchers;

import cern.acet.tracing.util.type.TypeConstraint;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class BulkConsumerTest {

    private static final String DOCUMENT_TYPE = "logalike";
    private static final ElasticsearchIndex DEFAULT_INDEX = ElasticsearchIndex.daily("default");
    private BulkConsumer bulkConsumer;
    private Client mockClient;

    private BulkProcessor mockProcessor;
    private IndexRequest mockRequest;
    private IndexRequestBuilder mockRequestBuilder;
    private ElasticsearchMessage mockMessage;
    private ImmutableMap<String, TypeConstraint<?>> typeMap = ImmutableMap.of();

    @Before
    public void setup() throws Exception {
        mockMessage = mock(ElasticsearchMessage.class);
        when(mockMessage.getIndices()).thenReturn(ImmutableList.<ElasticsearchIndex> of());
        when(mockMessage.getTimestamp()).thenReturn(Optional.empty());
        when(mockMessage.toMap()).thenReturn(new HashMap<String, Object>());
        when(mockMessage.getTypeMap()).thenReturn(typeMap);

        mockRequest = mock(IndexRequest.class);
        mockRequestBuilder = mock(IndexRequestBuilder.class);
        mockClient = mock(Client.class);
        mockProcessor = mock(BulkProcessor.class);
        bulkConsumer = new BulkConsumer(mockProcessor, DEFAULT_INDEX, DOCUMENT_TYPE);
    }

    @Test
    public void canCloseWithoutExceptions() throws Exception {
        bulkConsumer.close();
    }

    @Test
    public void canCreateFromClient() {
        Settings settings = Settings.EMPTY;
        when(mockClient.settings()).thenReturn(settings);
        bulkConsumer = new BulkConsumer(mockClient, DEFAULT_INDEX, Duration.ofMillis(1), DOCUMENT_TYPE);
        verify(mockClient).settings();
    }

    @Test
    public void canFlushBulkInIntervals() throws InterruptedException {
        when(mockClient.settings()).thenReturn(Settings.EMPTY);
        bulkConsumer = new BulkConsumer(mockClient, DEFAULT_INDEX, Duration.ofMillis(1), DOCUMENT_TYPE);
        bulkConsumer.accept(mockMessage);
        Thread.sleep(100);
        verify(mockClient).bulk(Matchers.any(), Matchers.any());
        bulkConsumer.accept(mockMessage);
        Thread.sleep(100);
        verify(mockClient, times(2)).bulk(Matchers.any(), Matchers.any());
    }

    @Test
    public void canStoreMessageInBulk() throws Exception {
        when(mockRequestBuilder.request()).thenReturn(mockRequest);
        bulkConsumer.accept(mockMessage);
        verify(mockProcessor).add(Matchers.any(IndexRequest.class));
    }

    @Test
    public void canFormatTimestamps() throws Exception {
        String field = "tracing_timestamp";
        ZonedDateTime dateTime = ZonedDateTime.now();
        Map<String, Object> objectMap = new HashMap<>();
        objectMap.put(field, dateTime);
        setTypeMap(ImmutableMap.of(field, TypeConstraint.ofClass(ZonedDateTime.class)));

        when(mockRequestBuilder.request()).thenReturn(mockRequest);
        when(mockMessage.toMap()).thenReturn(objectMap);
        when(mockMessage.containsKey(field)).thenReturn(true);
        when(mockMessage.getAs(field, ZonedDateTime.class)).thenReturn(dateTime);

        String output = getRequest().toString();
        assertTrue(output.contains(getJsonStringKeyValue(field, dateTime.format(BulkConsumer.DATE_TIME_FORMATTER))));
    }

    @Test
    public void canSetCorrectContent() throws Exception {
        when(mockRequestBuilder.request()).thenReturn(mockRequest);
        String field = "testField";
        Double data = Math.random();
        Map<String, Object> objectMap = new HashMap<>();
        objectMap.put(field, data);

        when(mockMessage.toMap()).thenReturn(objectMap);
        String output = getRequest().toString();
        assertTrue(output.contains(field));
        assertTrue(output.contains(data.toString()));
    }

    @Test
    public void canFormatTimestamp() {
        String date = "2015-09-30T12:31:21.021+02:00";
        String expected = "2015-09-30T12:31:21.021+0200";
        ZonedDateTime time = ZonedDateTime.parse(date);
        assertEquals(expected, BulkConsumer.formatTimestamp(time));
    }

    @Test
    public void canFormatTimestampWithoutMillis() {
        String date = "2015-09-30T12:31:21+02:00";
        String expected = "2015-09-30T12:31:21.000+0200";
        ZonedDateTime time = ZonedDateTime.parse(date);
        assertEquals(expected, BulkConsumer.formatTimestamp(time));
    }

    @Test
    public void canFormatTimestampWithoutTimezone() {
        String date = "2015-09-30T12:31:21Z";
        String expected = "2015-09-30T12:31:21.000+0000";
        ZonedDateTime time = ZonedDateTime.parse(date);
        assertEquals(expected, BulkConsumer.formatTimestamp(time));
    }

    @Test
    public void canSetCorrectTimestamp() throws Exception {
        String field = ElasticsearchMessage.DEFAULT_TIMESTAMP_FIELD;
        ZonedDateTime time = ZonedDateTime.now();
        setTypeMap(ImmutableMap.of(field, TypeConstraint.ofClass(ZonedDateTime.class)));
        Map<String, Object> objectMap = new HashMap<>();
        objectMap.put(field, time);

        when(mockRequestBuilder.request()).thenReturn(mockRequest);
        when(mockMessage.containsKey(field)).thenReturn(true);
        when(mockMessage.getAs(field, ZonedDateTime.class)).thenReturn(time);
        when(mockMessage.toMap()).thenReturn(objectMap);

        String output = getRequest().toString();
        assertTrue(output.contains(getJsonStringKeyValue(field, BulkConsumer.formatTimestamp(time))));
    }

    @Test
    public void canStoreWithCorrectDocumentType() throws Exception {
        ElasticsearchIndex index = ElasticsearchIndex.daily("testIndex");
        when(mockMessage.getIndices()).thenReturn(ImmutableList.of(index));
        IndexRequest request = getRequest();
        assertEquals(DOCUMENT_TYPE, request.type());
    }

    @Test
    public void canStoreToOneIndex() throws Exception {
        ElasticsearchIndex index = ElasticsearchIndex.daily("testIndex");
        when(mockMessage.getIndices()).thenReturn(ImmutableList.of(index));
        IndexRequest request = getRequest();
        assertEquals(1, request.indices().length);
        assertEquals(index.toString(), request.indices()[0]);
    }

    @Test
    public void canStoreToMultipleIndices() throws Exception {
        ElasticsearchIndex index1 = ElasticsearchIndex.daily("testIndex1");
        ElasticsearchIndex index2 = ElasticsearchIndex.daily("testIndex2");
        when(mockMessage.getIndices()).thenReturn(ImmutableList.of(index1, index2));
        bulkConsumer.accept(mockMessage);

        verify(mockProcessor).add(matchesIndex(index1));
        verify(mockProcessor).add(matchesIndex(index2));
    }

    private IndexRequest getRequest() {
        bulkConsumer.accept(mockMessage);
        ArgumentCaptor<IndexRequest> requestCaptor = ArgumentCaptor.forClass(IndexRequest.class);
        verify(mockProcessor).add(requestCaptor.capture());
        return requestCaptor.getValue();
    }

    private IndexRequest matchesIndex(final ElasticsearchIndex index) {
        return Matchers.argThat(new BaseMatcher<IndexRequest>() {

            @Override
            public void describeTo(Description description) {
                description.appendText("Index request on " + index);
            }

            @Override
            public boolean matches(Object item) {
                if (item instanceof IndexRequest) {
                    IndexRequest request = (IndexRequest) item;
                    return request.index().equals(index.toString());
                }
                return false;
            }

        });
    }

    private String getJsonStringKeyValue(String key, String value) {
        return String.format("\"%s\":\"%s\"", key, value);
    }

    private void setTypeMap(ImmutableMap<String, TypeConstraint<?>> map) {
        typeMap = map;
        when(mockMessage.getTypeMap()).thenReturn(typeMap);
    }

}

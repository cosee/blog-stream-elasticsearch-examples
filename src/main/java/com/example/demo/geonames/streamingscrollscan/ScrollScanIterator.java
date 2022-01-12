package com.example.demo.geonames.streamingscrollscan;

import lombok.SneakyThrows;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.SearchHits;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class ScrollScanIterator implements Iterator<SearchHits>, AutoCloseable {
    public static final TimeValue KEEP_ALIVE = TimeValue.timeValueMinutes(10);
    public final RestHighLevelClient client;
    public final SearchRequest searchRequest;

    private SearchResponse lastResponse;

    public ScrollScanIterator(RestHighLevelClient client, SearchRequest searchRequest) {
        this.client = client;
        this.searchRequest = searchRequest;
    }

    @Override
    public boolean hasNext() {
        return lastResponse == null || lastResponse.getHits().getHits().length > 0;
    }

    @SneakyThrows
    @Override
    public SearchHits next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        if (lastResponse == null) {
            lastResponse = getInitialResponse();
        } else {
            lastResponse = getNextScrollResponse();
        }
        return lastResponse.getHits();
    }

    private SearchResponse getInitialResponse() throws IOException {
        SearchRequest initialSearchRequest = new SearchRequest(searchRequest).scroll(KEEP_ALIVE);
        return client.search(initialSearchRequest, RequestOptions.DEFAULT);
    }

    private SearchResponse getNextScrollResponse() throws IOException {
        SearchScrollRequest scrollRequest = new SearchScrollRequest(lastResponse.getScrollId()).scroll(KEEP_ALIVE);
        return client.scroll(scrollRequest, RequestOptions.DEFAULT);
    }

    @SneakyThrows
    public void close() {
        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        clearScrollRequest.addScrollId(lastResponse.getScrollId());
        client.clearScroll(clearScrollRequest, RequestOptions.DEFAULT);
    }
}

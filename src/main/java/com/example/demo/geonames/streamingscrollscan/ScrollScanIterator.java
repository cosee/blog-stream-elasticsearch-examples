package com.example.demo.geonames.streamingscrollscan;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.Time;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class ScrollScanIterator<T> implements Iterator<SearchResponse<T>>, AutoCloseable {
    public static final Time KEEP_ALIVE = Time.of(t -> t.time("10s"));
    public final ElasticsearchClient client;
    public final SearchRequest searchRequest;
    private final Class<T> documentClass;

    private SearchResponse<T> lastResponse;


    public static <T> ScrollScanIterator<T> of(ElasticsearchClient client, SearchRequest searchRequest, Class<T> documentClass) {
        if (searchRequest.scroll() == null) {
            throw new IllegalArgumentException("Search request must have 'scroll()' set");
        }
        return new ScrollScanIterator<>(client, searchRequest, documentClass);
    }

    @Override
    public boolean hasNext() {
        return lastResponse == null || lastResponse.documents().size() > 0;
    }

    @SneakyThrows
    @Override
    public SearchResponse<T> next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        if (lastResponse == null) {
            lastResponse = getInitialResponse();
        } else {
            lastResponse = getNextScrollResponse();
        }
        return lastResponse;
    }

    private SearchResponse<T> getInitialResponse() throws IOException {
        return client.search(searchRequest, documentClass);
    }

    private SearchResponse<T> getNextScrollResponse() throws IOException {
        return client.scroll(s -> s.scrollId(lastResponse.scrollId()).scroll(KEEP_ALIVE),documentClass);
    }

    @SneakyThrows
    public void close() {
        client.clearScroll(c -> c.scrollId(lastResponse.scrollId()));
    }
}

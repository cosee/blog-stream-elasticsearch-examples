package com.example.demo.geonames;

import com.example.demo.geonames.model.PostalCode;
import com.example.demo.geonames.streamingscrollscan.ScrollScanIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.client.core.CountResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.stereotype.Service;

import javax.annotation.PreDestroy;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

@Service
public class PostalCodeElasticSearchService {

    public static final String INDEX_NAME = "postalcodes";
    public static final TimeValue KEEP_ALIVE = TimeValue.timeValueMinutes(10);
    public static final int SCROLL_PAGE_SIZE = 1000;
    private RestHighLevelClient client = new RestHighLevelClient(
            RestClient.builder(new HttpHost("localhost", 9200, "http"))
    );

    private ObjectMapper objectMapper = new ObjectMapper();

    @SneakyThrows
    public void addPostalCodes(List<PostalCode> postalCodeList) {
        BulkRequest request;
        request = new BulkRequest();
        for (PostalCode postalCode : postalCodeList) {
            request.add(
                    new IndexRequest(INDEX_NAME)
                            .id(postalCode.getId())
                            .source(objectMapper.writeValueAsString(postalCode), XContentType.JSON)
            );
        }
        BulkResponse bulk = client.bulk(request, RequestOptions.DEFAULT);
        for (BulkItemResponse item : bulk.getItems()) {
            if (item.getFailure() != null) {
                throw new IllegalStateException(item.getFailure().getCause());
            }
        }
    }

    @SneakyThrows
    public void refresh() {
        client.indices().refresh(new RefreshRequest(INDEX_NAME), RequestOptions.DEFAULT);
    }

    @SneakyThrows
    public List<PostalCode> fetchAllPostalCodes(String countryIso2) {
        SearchResponse response = client.search(
                buildSearchRequest(countryIso2),
                RequestOptions.DEFAULT
        );
        return extractPostalCodes(response.getHits());
    }

    private List<PostalCode> extractPostalCodes(SearchHits hits) {
        return Stream.of(hits.getHits())
                .map(this::searchHitToPostalCode)
                .collect(Collectors.toList());
    }

    private SearchRequest buildSearchRequest(String countryIso2) {
        return new SearchRequest(INDEX_NAME).source(
                new SearchSourceBuilder().query(
                        QueryBuilders.termsQuery("countryIso2.keyword", countryIso2)
                ).size(SCROLL_PAGE_SIZE)
        );
    }

    @SneakyThrows
    public List<PostalCode> fetchAllPostalCodesByScrollScan(String countryIso2) {

        SearchResponse initialResponse = client.search(
                buildSearchRequest(countryIso2)
                        .scroll(KEEP_ALIVE),
                RequestOptions.DEFAULT);
        List<PostalCode> result = new LinkedList<>(extractPostalCodes(initialResponse.getHits()));
        List<PostalCode> postalCodes;
        String scrollId = initialResponse.getScrollId();
        try {
            SearchResponse scrollResponse;
            do {
                scrollResponse = client.scroll(new SearchScrollRequest(scrollId).scroll(KEEP_ALIVE), RequestOptions.DEFAULT);
                postalCodes = extractPostalCodes(scrollResponse.getHits());
                scrollId = scrollResponse.getScrollId();
                result.addAll(postalCodes);
            } while (scrollResponse.getHits().getHits().length > 0);
        } finally {
            ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
            clearScrollRequest.addScrollId(scrollId);
            client.clearScroll(clearScrollRequest, RequestOptions.DEFAULT);
        }

        return result;
    }

    public Stream<PostalCode> streamAllPostalCodesByScrollScan(String countryIso2) {
        SearchRequest searchRequest = buildSearchRequest(countryIso2);
        return streamSearchHits(searchRequest);
    }

    private Stream<PostalCode> streamSearchHits(SearchRequest searchRequest) {
        ScrollScanIterator scrollScanIterator = new ScrollScanIterator(client, searchRequest);
        Spliterator<SearchHits> spliterator = Spliterators.spliteratorUnknownSize(scrollScanIterator, 0);
        return StreamSupport.stream(spliterator, false)
                .onClose(scrollScanIterator::close)
                .flatMap(searchHits -> Stream.of(searchHits.getHits()))
                .map(this::searchHitToPostalCode);
    }

    @SneakyThrows
    private PostalCode searchHitToPostalCode(SearchHit searchHit) {
        return objectMapper.readValue(searchHit.getSourceAsString(), PostalCode.class);
    }

    @SneakyThrows
    void deleteIndex() {
        DeleteIndexRequest request = new DeleteIndexRequest(INDEX_NAME);
        client.indices().delete(request, RequestOptions.DEFAULT);
    }

    @SneakyThrows
    public long countAllPostalCodes() {
        CountResponse count = client.count(new CountRequest(INDEX_NAME).query(QueryBuilders.matchAllQuery()), RequestOptions.DEFAULT);
        return count.getCount();
    }

    @PreDestroy
    private void shutdown() throws IOException {
        client.close();
    }
}
